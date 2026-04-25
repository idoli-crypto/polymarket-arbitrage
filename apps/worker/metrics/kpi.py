from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from apps.api.db.models import (
    DetectedOpportunity,
    KpiRunSummary,
    KpiSnapshot,
    OpportunityKpiSnapshot,
    SimulationResult,
)


KPI_VERSION = "kpi_v2"
MONEY_PRECISION = Decimal("0.0001")
RATIO_PRECISION = Decimal("0.0001")
ZERO = Decimal("0.0000")

FINAL_STATUS_VALID = "valid"
FINAL_STATUS_REJECTED = "rejected"

DECAY_STATUS_ALIVE = "alive"
DECAY_STATUS_DECAYED = "decayed"

STAGE_DETECTED = "detected"
STAGE_RULE_PASS = "rule_pass"
STAGE_SEMANTIC_PASS = "semantic_pass"
STAGE_RESOLUTION_PASS = "resolution_pass"
STAGE_EXECUTABLE_PASS = "executable_pass"
STAGE_SIMULATION_PASS = "simulation_pass"

REJECTION_STAGE_RULE = "rule"
REJECTION_STAGE_SEMANTIC = "semantic"
REJECTION_STAGE_RESOLUTION = "resolution"
REJECTION_STAGE_EXECUTABLE = "executable"
REJECTION_STAGE_SIMULATION = "simulation"
REJECTION_STAGE_PERSISTENCE = "persistence"
REJECTION_REASON_NO_LONGER_PRESENT = "no_longer_present"

STATUS_VALID = "valid"
STATUS_RISKY = "risky"


@dataclass(slots=True)
class OpportunityKpiSnapshotInput:
    opportunity_id: int
    event_group_key: str
    involved_market_ids: list[int]
    opportunity_type: str
    family: str
    relation_type: str | None
    relation_direction: str | None
    detection_window_start: datetime
    snapshot_timestamp: datetime
    final_status: str
    rejection_reason: str | None
    s_logic: Decimal | None
    s_sem: Decimal | None
    s_res: Decimal | None
    top_of_book_edge: Decimal | None
    depth_weighted_edge: Decimal | None
    fee_adjusted_edge: Decimal | None
    fill_completion_ratio: Decimal | None
    execution_feasible: bool | None
    capital_lock_estimate_hours: Decimal | None
    detector_version: str
    validation_version: str | None
    simulation_version: str | None
    rule_status: str | None
    semantic_status: str | None
    resolution_status: str | None
    execution_status: str | None
    simulation_status: str | None
    intended_size_usd: Decimal | None = None
    executable_size_usd: Decimal | None = None
    fee_cost_usd: Decimal | None = None
    slippage_cost_usd: Decimal | None = None


@dataclass(slots=True)
class PersistedKpiRunSummary:
    id: int
    created_at: datetime
    run_started_at: datetime
    run_completed_at: datetime
    total_opportunities: int
    valid_after_rule: int
    valid_after_semantic: int
    valid_after_resolution: int
    valid_after_executable: int
    valid_after_simulation: int
    avg_executable_edge: Decimal
    avg_fill_ratio: Decimal
    avg_capital_lock: Decimal
    false_positive_rate: Decimal
    family_distribution: dict[str, int]


@dataclass(slots=True)
class _SnapshotDraft:
    opportunity_id: int
    lineage_key: str
    family: str
    validation_stage_reached: str
    final_status: str
    rejection_stage: str | None
    rejection_reason: str | None
    detected: bool
    rule_pass: bool
    semantic_pass: bool
    resolution_pass: bool
    executable_pass: bool
    simulation_pass: bool
    s_logic: Decimal | None
    s_sem: Decimal | None
    s_res: Decimal | None
    top_of_book_edge: Decimal | None
    depth_weighted_edge: Decimal | None
    fee_adjusted_edge: Decimal | None
    fill_completion_ratio: Decimal | None
    execution_feasible: bool | None
    capital_lock_estimate_hours: Decimal | None
    detector_version: str
    validation_version: str | None
    simulation_version: str | None
    snapshot_timestamp: datetime
    first_seen_timestamp: datetime
    last_seen_timestamp: datetime
    persistence_duration_seconds: int
    decay_status: str
    raw_context: dict[str, object]


def persist_kpi_run(
    session: Session,
    inputs: list[OpportunityKpiSnapshotInput],
    *,
    run_started_at: datetime,
    run_completed_at: datetime,
) -> PersistedKpiRunSummary | None:
    if not inputs:
        return None

    latest_by_lineage = _load_latest_snapshots_by_lineage(session)
    actual_snapshots: list[_SnapshotDraft] = []
    absence_decay_snapshots: list[_SnapshotDraft] = []

    grouped_inputs: dict[datetime, list[OpportunityKpiSnapshotInput]] = defaultdict(list)
    for item in inputs:
        grouped_inputs[item.detection_window_start].append(item)

    for detection_window_start in sorted(grouped_inputs):
        normalized_window_start = _normalize_datetime(detection_window_start)
        window_inputs = sorted(
            grouped_inputs[detection_window_start],
            key=lambda item: (item.snapshot_timestamp, item.opportunity_id),
        )
        observed_lineages = {_build_lineage_key(item) for item in window_inputs}

        for lineage_key, previous in sorted(latest_by_lineage.items()):
            if previous.decay_status != DECAY_STATUS_ALIVE:
                continue
            if _normalize_datetime(previous.last_seen_timestamp) >= normalized_window_start:
                continue
            if lineage_key in observed_lineages:
                continue

            decay_snapshot = _build_absence_decay_snapshot(
                previous,
                snapshot_timestamp=normalized_window_start,
            )
            absence_decay_snapshots.append(decay_snapshot)
            latest_by_lineage[lineage_key] = decay_snapshot

        for item in window_inputs:
            lineage_key = _build_lineage_key(item)
            current_snapshot = _build_current_snapshot(
                item,
                previous=latest_by_lineage.get(lineage_key),
            )
            actual_snapshots.append(current_snapshot)
            latest_by_lineage[lineage_key] = current_snapshot

    run_summary = KpiRunSummary(
        run_started_at=run_started_at,
        run_completed_at=run_completed_at,
        kpi_version=KPI_VERSION,
        total_opportunities=len(actual_snapshots),
        valid_after_rule=sum(1 for snapshot in actual_snapshots if snapshot.rule_pass),
        valid_after_semantic=sum(1 for snapshot in actual_snapshots if snapshot.semantic_pass),
        valid_after_resolution=sum(1 for snapshot in actual_snapshots if snapshot.resolution_pass),
        valid_after_executable=sum(1 for snapshot in actual_snapshots if snapshot.executable_pass),
        valid_after_simulation=sum(1 for snapshot in actual_snapshots if snapshot.simulation_pass),
        avg_executable_edge=_average_decimal(snapshot.fee_adjusted_edge for snapshot in actual_snapshots),
        avg_fill_ratio=_average_decimal(snapshot.fill_completion_ratio for snapshot in actual_snapshots),
        avg_capital_lock=_average_decimal(
            snapshot.capital_lock_estimate_hours for snapshot in actual_snapshots
        ),
        false_positive_rate=_quantize_ratio(
            _safe_divide(
                sum(1 for snapshot in actual_snapshots if snapshot.final_status == FINAL_STATUS_REJECTED),
                len(actual_snapshots),
            )
        ),
        family_distribution=dict(sorted(Counter(snapshot.family for snapshot in actual_snapshots).items())),
        detector_versions_json=sorted({snapshot.detector_version for snapshot in actual_snapshots}),
        validation_versions_json=sorted(
            {snapshot.validation_version for snapshot in actual_snapshots if snapshot.validation_version}
        ),
        simulation_versions_json=sorted(
            {snapshot.simulation_version for snapshot in actual_snapshots if snapshot.simulation_version}
        ),
        raw_context=_build_run_summary_context(
            actual_snapshots=actual_snapshots,
            absence_decay_snapshots=absence_decay_snapshots,
            run_started_at=run_started_at,
            run_completed_at=run_completed_at,
        ),
    )
    session.add(run_summary)
    session.flush()

    persisted_snapshots = [
        _persist_snapshot(session, run_summary.id, snapshot)
        for snapshot in [*actual_snapshots, *absence_decay_snapshots]
    ]

    legacy_snapshot = _persist_legacy_snapshot(
        session,
        run_summary=run_summary,
        actual_snapshots=actual_snapshots,
    )

    run_summary.raw_context = {
        **run_summary.raw_context,
        "opportunity_kpi_snapshot_ids": [snapshot.id for snapshot in persisted_snapshots],
        "legacy_kpi_snapshot_id": legacy_snapshot.id,
    }
    session.flush()

    return PersistedKpiRunSummary(
        id=run_summary.id,
        created_at=run_summary.created_at,
        run_started_at=run_summary.run_started_at,
        run_completed_at=run_summary.run_completed_at,
        total_opportunities=run_summary.total_opportunities,
        valid_after_rule=run_summary.valid_after_rule,
        valid_after_semantic=run_summary.valid_after_semantic,
        valid_after_resolution=run_summary.valid_after_resolution,
        valid_after_executable=run_summary.valid_after_executable,
        valid_after_simulation=run_summary.valid_after_simulation,
        avg_executable_edge=run_summary.avg_executable_edge,
        avg_fill_ratio=run_summary.avg_fill_ratio,
        avg_capital_lock=run_summary.avg_capital_lock,
        false_positive_rate=run_summary.false_positive_rate,
        family_distribution=run_summary.family_distribution,
    )


def calculate_and_persist_kpi_snapshot(session: Session) -> PersistedKpiRunSummary | None:
    opportunities = session.scalars(
        select(DetectedOpportunity)
        .where(DetectedOpportunity.validation_status.is_not(None))
        .order_by(DetectedOpportunity.validated_at.asc(), DetectedOpportunity.id.asc())
    ).all()
    latest_simulation_results = _load_latest_simulation_results(session, [opportunity.id for opportunity in opportunities])
    inputs = [
        _build_input_from_opportunity(
            opportunity,
            latest_simulation_results.get(opportunity.id),
        )
        for opportunity in opportunities
    ]
    inputs = [item for item in inputs if item is not None]
    if not inputs:
        return None

    run_started_at = min(item.snapshot_timestamp for item in inputs)
    run_completed_at = max(item.snapshot_timestamp for item in inputs)
    return persist_kpi_run(
        session,
        inputs,
        run_started_at=run_started_at,
        run_completed_at=run_completed_at,
    )


def _build_input_from_opportunity(
    opportunity: DetectedOpportunity,
    simulation_result: SimulationResult | None,
) -> OpportunityKpiSnapshotInput | None:
    if opportunity.validation_status is None:
        return None

    simulation_details = {}
    if isinstance(opportunity.raw_context, dict):
        simulation_details = opportunity.raw_context.get("simulation_validation_details", {}) or {}

    return OpportunityKpiSnapshotInput(
        opportunity_id=opportunity.id,
        event_group_key=opportunity.event_group_key,
        involved_market_ids=list(opportunity.involved_market_ids),
        opportunity_type=opportunity.opportunity_type,
        family=opportunity.family,
        relation_type=opportunity.relation_type,
        relation_direction=opportunity.relation_direction,
        detection_window_start=opportunity.detection_window_start,
        snapshot_timestamp=opportunity.validated_at or opportunity.detected_at,
        final_status=opportunity.validation_status,
        rejection_reason=opportunity.validation_reason,
        s_logic=opportunity.s_logic,
        s_sem=opportunity.s_sem,
        s_res=opportunity.s_res,
        top_of_book_edge=opportunity.top_of_book_edge,
        depth_weighted_edge=opportunity.depth_weighted_edge,
        fee_adjusted_edge=opportunity.fee_adjusted_edge,
        fill_completion_ratio=(
            simulation_result.fill_completion_ratio
            if simulation_result is not None
            else _to_decimal(simulation_details.get("fill_completion_ratio"))
        ),
        execution_feasible=(
            simulation_result.execution_feasible
            if simulation_result is not None
            else _to_bool(simulation_details.get("execution_feasible"))
        ),
        capital_lock_estimate_hours=(
            simulation_result.capital_lock_estimate_hours
            if simulation_result is not None
            else _to_decimal(simulation_details.get("capital_lock_estimate_hours"))
        ),
        detector_version=opportunity.detector_version,
        validation_version=opportunity.validation_version,
        simulation_version=opportunity.simulation_version,
        rule_status=_context_status(opportunity.raw_context, "rule_validation_status"),
        semantic_status=_context_status(opportunity.raw_context, "semantic_validation_status"),
        resolution_status=_context_status(opportunity.raw_context, "resolution_validation_status"),
        execution_status=_context_status(opportunity.raw_context, "execution_validation_status"),
        simulation_status=_context_status(opportunity.raw_context, "simulation_validation_status"),
        intended_size_usd=_to_decimal(simulation_details.get("intended_size_usd")),
        executable_size_usd=(
            simulation_result.min_executable_size
            if simulation_result is not None
            else None
        ),
    )


def _build_current_snapshot(
    item: OpportunityKpiSnapshotInput,
    *,
    previous: _SnapshotDraft | OpportunityKpiSnapshot | None,
) -> _SnapshotDraft:
    rule_pass = item.rule_status == STATUS_VALID
    semantic_pass = rule_pass and item.semantic_status == STATUS_VALID
    resolution_pass = semantic_pass and item.resolution_status == STATUS_VALID
    executable_pass = resolution_pass and item.execution_status == STATUS_VALID
    simulation_pass = executable_pass and item.simulation_status == STATUS_VALID

    validation_stage_reached = _derive_validation_stage_reached(
        rule_pass=rule_pass,
        semantic_pass=semantic_pass,
        resolution_pass=resolution_pass,
        executable_pass=executable_pass,
        simulation_pass=simulation_pass,
    )
    rejection_stage = _derive_rejection_stage(
        final_status=item.final_status,
        rule_pass=rule_pass,
        semantic_status=item.semantic_status,
        resolution_status=item.resolution_status,
        execution_status=item.execution_status,
        simulation_status=item.simulation_status,
    )
    prior_first_seen = None
    if previous is not None and previous.decay_status == DECAY_STATUS_ALIVE:
        prior_first_seen = _normalize_datetime(previous.first_seen_timestamp)
    first_seen_timestamp = prior_first_seen or _normalize_datetime(item.snapshot_timestamp)
    last_seen_timestamp = _normalize_datetime(item.snapshot_timestamp)

    intended_size_usd = _quantize_money(item.intended_size_usd) if item.intended_size_usd is not None else None
    executable_size_usd = (
        _quantize_money(item.executable_size_usd) if item.executable_size_usd is not None else None
    )

    return _SnapshotDraft(
        opportunity_id=item.opportunity_id,
        lineage_key=_build_lineage_key(item),
        family=item.family,
        validation_stage_reached=validation_stage_reached,
        final_status=item.final_status,
        rejection_stage=rejection_stage,
        rejection_reason=item.rejection_reason,
        detected=True,
        rule_pass=rule_pass,
        semantic_pass=semantic_pass,
        resolution_pass=resolution_pass,
        executable_pass=executable_pass,
        simulation_pass=simulation_pass,
        s_logic=_quantize_optional(item.s_logic),
        s_sem=_quantize_optional(item.s_sem),
        s_res=_quantize_optional(item.s_res),
        top_of_book_edge=_quantize_optional(item.top_of_book_edge),
        depth_weighted_edge=_quantize_optional(item.depth_weighted_edge),
        fee_adjusted_edge=_quantize_optional(item.fee_adjusted_edge),
        fill_completion_ratio=_quantize_optional(item.fill_completion_ratio),
        execution_feasible=item.execution_feasible,
        capital_lock_estimate_hours=_quantize_optional(item.capital_lock_estimate_hours),
        detector_version=item.detector_version,
        validation_version=item.validation_version,
        simulation_version=item.simulation_version,
        snapshot_timestamp=_normalize_datetime(item.snapshot_timestamp),
        first_seen_timestamp=first_seen_timestamp,
        last_seen_timestamp=last_seen_timestamp,
        persistence_duration_seconds=_duration_seconds(first_seen_timestamp, last_seen_timestamp),
        decay_status=(
            DECAY_STATUS_ALIVE if item.final_status == FINAL_STATUS_VALID else DECAY_STATUS_DECAYED
        ),
        raw_context={
            "kpi_version": KPI_VERSION,
            "event_group_key": item.event_group_key,
            "opportunity_type": item.opportunity_type,
            "relation_type": item.relation_type,
            "relation_direction": item.relation_direction,
            "involved_market_ids": list(item.involved_market_ids),
            "lineage_components": _lineage_components(item),
            "stage_statuses": {
                "rule": item.rule_status,
                "semantic": item.semantic_status,
                "resolution": item.resolution_status,
                "executable": item.execution_status,
                "simulation": item.simulation_status,
            },
            "metric_sources": {
                "avg_executable_edge_source": "fee_adjusted_edge",
                "fill_ratio_source": "simulation_validation.fill_completion_ratio",
                "capital_lock_source": "simulation_validation.capital_lock_estimate_hours",
            },
            "simulation_metrics": {
                "intended_size_usd": format(intended_size_usd, "f") if intended_size_usd is not None else None,
                "executable_size_usd": (
                    format(executable_size_usd, "f") if executable_size_usd is not None else None
                ),
                "fee_cost_usd": format(_quantize_money(item.fee_cost_usd), "f")
                if item.fee_cost_usd is not None
                else None,
                "slippage_cost_usd": format(_quantize_money(item.slippage_cost_usd), "f")
                if item.slippage_cost_usd is not None
                else None,
            },
            "decay_basis": "currently_valid" if item.final_status == FINAL_STATUS_VALID else "currently_rejected",
        },
    )


def _build_absence_decay_snapshot(
    previous: _SnapshotDraft | OpportunityKpiSnapshot,
    *,
    snapshot_timestamp: datetime,
) -> _SnapshotDraft:
    return _SnapshotDraft(
        opportunity_id=previous.opportunity_id,
        lineage_key=previous.lineage_key,
        family=previous.family,
        validation_stage_reached=previous.validation_stage_reached,
        final_status=FINAL_STATUS_REJECTED,
        rejection_stage=REJECTION_STAGE_PERSISTENCE,
        rejection_reason=REJECTION_REASON_NO_LONGER_PRESENT,
        detected=previous.detected,
        rule_pass=previous.rule_pass,
        semantic_pass=previous.semantic_pass,
        resolution_pass=previous.resolution_pass,
        executable_pass=previous.executable_pass,
        simulation_pass=previous.simulation_pass,
        s_logic=previous.s_logic,
        s_sem=previous.s_sem,
        s_res=previous.s_res,
        top_of_book_edge=previous.top_of_book_edge,
        depth_weighted_edge=previous.depth_weighted_edge,
        fee_adjusted_edge=previous.fee_adjusted_edge,
        fill_completion_ratio=previous.fill_completion_ratio,
        execution_feasible=previous.execution_feasible,
        capital_lock_estimate_hours=previous.capital_lock_estimate_hours,
        detector_version=previous.detector_version,
        validation_version=previous.validation_version,
        simulation_version=previous.simulation_version,
        snapshot_timestamp=_normalize_datetime(snapshot_timestamp),
        first_seen_timestamp=_normalize_datetime(previous.first_seen_timestamp),
        last_seen_timestamp=_normalize_datetime(previous.last_seen_timestamp),
        persistence_duration_seconds=previous.persistence_duration_seconds,
        decay_status=DECAY_STATUS_DECAYED,
        raw_context={
            **dict(previous.raw_context or {}),
            "kpi_version": KPI_VERSION,
            "decay_basis": "absent_from_newer_detection_window",
            "decay_observed_at": snapshot_timestamp.isoformat(),
            "previous_snapshot_timestamp": previous.snapshot_timestamp.isoformat(),
        },
    )


def _derive_validation_stage_reached(
    *,
    rule_pass: bool,
    semantic_pass: bool,
    resolution_pass: bool,
    executable_pass: bool,
    simulation_pass: bool,
) -> str:
    if simulation_pass:
        return STAGE_SIMULATION_PASS
    if executable_pass:
        return STAGE_EXECUTABLE_PASS
    if resolution_pass:
        return STAGE_RESOLUTION_PASS
    if semantic_pass:
        return STAGE_SEMANTIC_PASS
    if rule_pass:
        return STAGE_RULE_PASS
    return STAGE_DETECTED


def _derive_rejection_stage(
    *,
    final_status: str,
    rule_pass: bool,
    semantic_status: str | None,
    resolution_status: str | None,
    execution_status: str | None,
    simulation_status: str | None,
) -> str | None:
    if final_status != FINAL_STATUS_REJECTED:
        return None
    if not rule_pass:
        return REJECTION_STAGE_RULE
    if semantic_status != STATUS_VALID:
        return REJECTION_STAGE_SEMANTIC
    if resolution_status != STATUS_VALID:
        return REJECTION_STAGE_RESOLUTION
    if execution_status != STATUS_VALID:
        return REJECTION_STAGE_EXECUTABLE
    if simulation_status != STATUS_VALID:
        return REJECTION_STAGE_SIMULATION
    return REJECTION_STAGE_PERSISTENCE


def _build_run_summary_context(
    *,
    actual_snapshots: list[_SnapshotDraft],
    absence_decay_snapshots: list[_SnapshotDraft],
    run_started_at: datetime,
    run_completed_at: datetime,
) -> dict[str, object]:
    rejection_distribution = Counter(
        snapshot.rejection_stage
        for snapshot in actual_snapshots
        if snapshot.rejection_stage is not None
    )
    metric_denominators = {
        "avg_executable_edge_count": sum(
            1 for snapshot in actual_snapshots if snapshot.fee_adjusted_edge is not None
        ),
        "avg_fill_ratio_count": sum(
            1 for snapshot in actual_snapshots if snapshot.fill_completion_ratio is not None
        ),
        "avg_capital_lock_count": sum(
            1 for snapshot in actual_snapshots if snapshot.capital_lock_estimate_hours is not None
        ),
    }
    total_intended_capital = _quantize_money(
        sum((_simulation_metric(snapshot, "intended_size_usd") for snapshot in actual_snapshots), start=ZERO)
    )
    total_executable_capital = _quantize_money(
        sum((_simulation_metric(snapshot, "executable_size_usd") for snapshot in actual_snapshots), start=ZERO)
    )
    executable_opportunities = sum(
        1 for snapshot in actual_snapshots if snapshot.execution_feasible is True
    )
    partial_opportunities = sum(
        1
        for snapshot in actual_snapshots
        if snapshot.fill_completion_ratio is not None
        and ZERO < snapshot.fill_completion_ratio < Decimal("1.0000")
    )
    rejected_opportunities = sum(
        1 for snapshot in actual_snapshots if snapshot.final_status == FINAL_STATUS_REJECTED
    )

    return {
        "kpi_version": KPI_VERSION,
        "run_started_at": run_started_at.isoformat(),
        "run_completed_at": run_completed_at.isoformat(),
        "processed_opportunity_ids": [snapshot.opportunity_id for snapshot in actual_snapshots],
        "processed_lineage_keys": [snapshot.lineage_key for snapshot in actual_snapshots],
        "absence_decay_lineage_keys": [snapshot.lineage_key for snapshot in absence_decay_snapshots],
        "absence_decay_count": len(absence_decay_snapshots),
        "rejection_distribution": dict(sorted(rejection_distribution.items())),
        "metric_denominators": metric_denominators,
        "legacy_projection": {
            "avg_real_edge_source": "avg_executable_edge",
            "valid_opportunities_source": "valid_after_simulation",
            "executable_opportunities": executable_opportunities,
            "partial_opportunities": partial_opportunities,
            "rejected_opportunities": rejected_opportunities,
            "total_intended_capital": format(total_intended_capital, "f"),
            "total_executable_capital": format(total_executable_capital, "f"),
        },
    }


def _persist_snapshot(
    session: Session,
    run_summary_id: int,
    draft: _SnapshotDraft,
) -> OpportunityKpiSnapshot:
    snapshot = OpportunityKpiSnapshot(
        run_summary_id=run_summary_id,
        opportunity_id=draft.opportunity_id,
        lineage_key=draft.lineage_key,
        kpi_version=KPI_VERSION,
        snapshot_timestamp=draft.snapshot_timestamp,
        family=draft.family,
        validation_stage_reached=draft.validation_stage_reached,
        final_status=draft.final_status,
        rejection_stage=draft.rejection_stage,
        rejection_reason=draft.rejection_reason,
        detected=draft.detected,
        rule_pass=draft.rule_pass,
        semantic_pass=draft.semantic_pass,
        resolution_pass=draft.resolution_pass,
        executable_pass=draft.executable_pass,
        simulation_pass=draft.simulation_pass,
        s_logic=draft.s_logic,
        s_sem=draft.s_sem,
        s_res=draft.s_res,
        top_of_book_edge=draft.top_of_book_edge,
        depth_weighted_edge=draft.depth_weighted_edge,
        fee_adjusted_edge=draft.fee_adjusted_edge,
        fill_completion_ratio=draft.fill_completion_ratio,
        execution_feasible=draft.execution_feasible,
        capital_lock_estimate_hours=draft.capital_lock_estimate_hours,
        detector_version=draft.detector_version,
        validation_version=draft.validation_version,
        simulation_version=draft.simulation_version,
        first_seen_timestamp=draft.first_seen_timestamp,
        last_seen_timestamp=draft.last_seen_timestamp,
        persistence_duration_seconds=draft.persistence_duration_seconds,
        decay_status=draft.decay_status,
        raw_context=draft.raw_context,
    )
    session.add(snapshot)
    session.flush()
    return snapshot


def _persist_legacy_snapshot(
    session: Session,
    *,
    run_summary: KpiRunSummary,
    actual_snapshots: list[_SnapshotDraft],
) -> KpiSnapshot:
    total_intended_capital = _quantize_money(
        sum((_simulation_metric(snapshot, "intended_size_usd") for snapshot in actual_snapshots), start=ZERO)
    )
    total_executable_capital = _quantize_money(
        sum((_simulation_metric(snapshot, "executable_size_usd") for snapshot in actual_snapshots), start=ZERO)
    )
    legacy_snapshot = KpiSnapshot(
        total_opportunities=run_summary.total_opportunities,
        valid_opportunities=run_summary.valid_after_simulation,
        executable_opportunities=sum(1 for snapshot in actual_snapshots if snapshot.execution_feasible is True),
        partial_opportunities=sum(
            1
            for snapshot in actual_snapshots
            if snapshot.fill_completion_ratio is not None
            and ZERO < snapshot.fill_completion_ratio < Decimal("1.0000")
        ),
        rejected_opportunities=sum(
            1 for snapshot in actual_snapshots if snapshot.final_status == FINAL_STATUS_REJECTED
        ),
        avg_real_edge=run_summary.avg_executable_edge,
        avg_fill_ratio=run_summary.avg_fill_ratio,
        false_positive_rate=run_summary.false_positive_rate,
        total_intended_capital=total_intended_capital,
        total_executable_capital=total_executable_capital,
        raw_context={
            "kpi_version": KPI_VERSION,
            "kpi_run_summary_id": run_summary.id,
            "legacy_projection_source": "kpi_run_summary",
            "valid_after_rule": run_summary.valid_after_rule,
            "valid_after_semantic": run_summary.valid_after_semantic,
            "valid_after_resolution": run_summary.valid_after_resolution,
            "valid_after_executable": run_summary.valid_after_executable,
            "valid_after_simulation": run_summary.valid_after_simulation,
        },
    )
    session.add(legacy_snapshot)
    session.flush()
    return legacy_snapshot


def _load_latest_snapshots_by_lineage(session: Session) -> dict[str, OpportunityKpiSnapshot]:
    ranked_snapshots = (
        select(
            OpportunityKpiSnapshot.id.label("snapshot_id"),
            OpportunityKpiSnapshot.lineage_key.label("lineage_key"),
            func.row_number()
            .over(
                partition_by=OpportunityKpiSnapshot.lineage_key,
                order_by=(
                    OpportunityKpiSnapshot.snapshot_timestamp.desc(),
                    OpportunityKpiSnapshot.id.desc(),
                ),
            )
            .label("snapshot_rank"),
        )
        .subquery()
    )
    rows = session.execute(
        select(OpportunityKpiSnapshot)
        .join(ranked_snapshots, ranked_snapshots.c.snapshot_id == OpportunityKpiSnapshot.id)
        .where(ranked_snapshots.c.snapshot_rank == 1)
    ).scalars()
    return {row.lineage_key: row for row in rows}


def _load_latest_simulation_results(
    session: Session,
    opportunity_ids: list[int],
) -> dict[int, SimulationResult]:
    if not opportunity_ids:
        return {}

    ranked_results = (
        select(
            SimulationResult.id.label("result_id"),
            SimulationResult.opportunity_id.label("opportunity_id"),
            func.row_number()
            .over(
                partition_by=SimulationResult.opportunity_id,
                order_by=(SimulationResult.created_at.desc(), SimulationResult.id.desc()),
            )
            .label("result_rank"),
        )
        .where(SimulationResult.opportunity_id.in_(opportunity_ids))
        .subquery()
    )
    rows = session.execute(
        select(SimulationResult)
        .join(ranked_results, ranked_results.c.result_id == SimulationResult.id)
        .where(ranked_results.c.result_rank == 1)
    ).scalars()
    return {row.opportunity_id: row for row in rows}


def _lineage_components(item: OpportunityKpiSnapshotInput) -> dict[str, object]:
    return {
        "family": item.family,
        "event_group_key": item.event_group_key,
        "opportunity_type": item.opportunity_type,
        "relation_type": item.relation_type,
        "relation_direction": item.relation_direction,
        "involved_market_ids": list(item.involved_market_ids),
        "detector_version": item.detector_version,
    }


def _build_lineage_key(item: OpportunityKpiSnapshotInput) -> str:
    encoded = json.dumps(_lineage_components(item), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _simulation_metric(snapshot: _SnapshotDraft, field_name: str) -> Decimal:
    simulation_metrics = {}
    if isinstance(snapshot.raw_context, dict):
        simulation_metrics = snapshot.raw_context.get("simulation_metrics", {}) or {}
    return _to_decimal(simulation_metrics.get(field_name)) or ZERO


def _context_status(raw_context: dict | None, key: str) -> str | None:
    if not isinstance(raw_context, dict):
        return None
    value = raw_context.get(key)
    return value if isinstance(value, str) else None


def _to_decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _to_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _duration_seconds(first_seen_timestamp: datetime, last_seen_timestamp: datetime) -> int:
    normalized_first_seen = _normalize_datetime(first_seen_timestamp)
    normalized_last_seen = _normalize_datetime(last_seen_timestamp)
    return max(0, int((normalized_last_seen - normalized_first_seen).total_seconds()))


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _safe_divide(numerator: Decimal | int, denominator: Decimal | int) -> Decimal:
    numerator_decimal = Decimal(numerator)
    denominator_decimal = Decimal(denominator)
    if denominator_decimal <= ZERO:
        return ZERO
    return numerator_decimal / denominator_decimal


def _average_decimal(values) -> Decimal:
    non_null_values = [value for value in values if value is not None]
    if not non_null_values:
        return ZERO
    total = sum((Decimal(value) for value in non_null_values), start=ZERO)
    return _quantize_ratio(_safe_divide(total, len(non_null_values)))


def _quantize_money(value: Decimal | int | None) -> Decimal:
    return Decimal(value or ZERO).quantize(MONEY_PRECISION)


def _quantize_ratio(value: Decimal | int | None) -> Decimal:
    return Decimal(value or ZERO).quantize(RATIO_PRECISION)


def _quantize_optional(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return _quantize_ratio(value)
