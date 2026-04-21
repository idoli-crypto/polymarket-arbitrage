from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any


SIMULATION_VERSION = "execution_sim_v1"
SUPPORTED_OPPORTUNITY_TYPE = "neg_risk_long_yes_bundle"
DEFAULT_INTENDED_SIZE_USD = Decimal("100.0000")
SIMULATION_STATUS_EXECUTABLE = "executable"
SIMULATION_STATUS_PARTIALLY_EXECUTABLE = "partially_executable"
SIMULATION_STATUS_REJECTED = "rejected"
SIMULATION_REASON_EXECUTABLE = "executable"
SIMULATION_REASON_PARTIALLY_EXECUTABLE = "partially_executable"
SIMULATION_REASON_INSUFFICIENT_DEPTH = "insufficient_depth"
SIMULATION_REASON_MISSING_SNAPSHOT = "missing_snapshot"
SIMULATION_REASON_STALE_SNAPSHOT_PLACEHOLDER = "stale_snapshot_placeholder"
SIMULATION_REASON_INSUFFICIENT_EDGE = "insufficient_edge"
SIMULATION_REASON_UNSUPPORTED_OPPORTUNITY_TYPE = "unsupported_opportunity_type"
MONEY_PRECISION = Decimal("0.0001")
RATIO_PRECISION = Decimal("0.0001")
BUNDLE_PAYOUT_USD = Decimal("1.0000")
ZERO = Decimal("0.0000")


@dataclass(slots=True)
class SimulationOpportunityInput:
    opportunity_id: int
    event_group_key: str
    involved_market_ids: list[int]
    opportunity_type: str
    gross_price_sum: Decimal
    detection_window_start: datetime
    raw_context: dict[str, Any] | None


@dataclass(slots=True)
class SimulationSnapshotInput:
    snapshot_id: int
    market_id: int
    captured_at: datetime
    best_bid: Decimal | None
    best_ask: Decimal | None
    bid_depth_usd: Decimal | None
    ask_depth_usd: Decimal | None


@dataclass(slots=True)
class ExecutionSimulationResult:
    simulation_status: str
    intended_size_usd: Decimal
    executable_size_usd: Decimal
    gross_cost_usd: Decimal
    gross_payout_usd: Decimal
    estimated_fees_usd: Decimal
    estimated_slippage_usd: Decimal
    estimated_net_edge_usd: Decimal
    fill_completion_ratio: Decimal
    simulation_reason: str
    raw_context: dict[str, Any]


def simulate_validated_opportunity(
    opportunity: SimulationOpportunityInput,
    *,
    latest_snapshots: dict[int, SimulationSnapshotInput | None],
    intended_size_usd: Decimal = DEFAULT_INTENDED_SIZE_USD,
) -> ExecutionSimulationResult:
    normalized_intended_size = _normalize_intended_size(intended_size_usd)
    base_context = {
        "simulation_version": SIMULATION_VERSION,
        "size_basis": "bundle_payout_notional_usd",
        "pricing_basis": "latest_yes_best_ask_and_top_of_book_ask_depth",
        "fee_model": "placeholder_zero",
        "slippage_model": "placeholder_zero",
        "stale_snapshot_status": SIMULATION_REASON_STALE_SNAPSHOT_PLACEHOLDER,
        "requested_intended_size_usd": format(normalized_intended_size, "f"),
        "opportunity_gross_price_sum": format(_quantize_money(opportunity.gross_price_sum), "f"),
    }

    if opportunity.opportunity_type != SUPPORTED_OPPORTUNITY_TYPE:
        return _rejected_result(
            intended_size_usd=normalized_intended_size,
            simulation_reason=SIMULATION_REASON_UNSUPPORTED_OPPORTUNITY_TYPE,
            raw_context={
                **base_context,
                "unsupported_opportunity_type": opportunity.opportunity_type,
            },
        )

    missing_snapshot_market_ids = [
        market_id
        for market_id in opportunity.involved_market_ids
        if latest_snapshots.get(market_id) is None
    ]
    if missing_snapshot_market_ids:
        return _rejected_result(
            intended_size_usd=normalized_intended_size,
            simulation_reason=SIMULATION_REASON_MISSING_SNAPSHOT,
            raw_context={
                **base_context,
                "missing_snapshot_market_ids": missing_snapshot_market_ids,
            },
        )

    per_market_context: list[dict[str, Any]] = []
    selected_snapshot_ids: dict[str, int] = {}
    limiting_bundle_size_usd: Decimal | None = None
    executable_market_ids: list[int] = []
    current_gross_price_sum = ZERO

    for market_id in opportunity.involved_market_ids:
        snapshot = latest_snapshots[market_id]
        assert snapshot is not None

        selected_snapshot_ids[str(market_id)] = snapshot.snapshot_id
        max_bundle_size_usd = _max_bundle_size_from_snapshot(snapshot)
        if max_bundle_size_usd > ZERO:
            executable_market_ids.append(market_id)

        current_gross_price_sum = _quantize_money(current_gross_price_sum + _positive_or_zero(snapshot.best_ask))
        if limiting_bundle_size_usd is None:
            limiting_bundle_size_usd = max_bundle_size_usd
        else:
            limiting_bundle_size_usd = min(limiting_bundle_size_usd, max_bundle_size_usd)

        per_market_context.append(
            {
                "market_id": market_id,
                "snapshot_id": snapshot.snapshot_id,
                "captured_at": snapshot.captured_at.isoformat(),
                "best_ask": _decimal_to_string(snapshot.best_ask),
                "ask_depth_usd": _decimal_to_string(snapshot.ask_depth_usd),
                "max_bundle_size_usd": format(max_bundle_size_usd, "f"),
            }
        )

    assert limiting_bundle_size_usd is not None

    executable_size_usd = _quantize_money(min(normalized_intended_size, limiting_bundle_size_usd))
    fill_completion_ratio = _quantize_ratio(executable_size_usd / normalized_intended_size)
    gross_cost_usd = _quantize_money(executable_size_usd * current_gross_price_sum)
    gross_payout_usd = _quantize_money(executable_size_usd * BUNDLE_PAYOUT_USD)
    estimated_fees_usd = ZERO
    estimated_slippage_usd = ZERO
    estimated_net_edge_usd = _quantize_money(
        gross_payout_usd - gross_cost_usd - estimated_fees_usd - estimated_slippage_usd
    )

    raw_context = {
        **base_context,
        "selected_snapshot_ids": selected_snapshot_ids,
        "per_market_execution": per_market_context,
        "current_gross_price_sum": format(current_gross_price_sum, "f"),
        "max_executable_bundle_size_usd": format(_quantize_money(limiting_bundle_size_usd), "f"),
        "executable_market_ids": executable_market_ids,
    }

    if executable_size_usd <= ZERO:
        return _rejected_result(
            intended_size_usd=normalized_intended_size,
            simulation_reason=SIMULATION_REASON_INSUFFICIENT_DEPTH,
            raw_context=raw_context,
        )

    if current_gross_price_sum >= BUNDLE_PAYOUT_USD or estimated_net_edge_usd <= ZERO:
        return ExecutionSimulationResult(
            simulation_status=SIMULATION_STATUS_REJECTED,
            intended_size_usd=normalized_intended_size,
            executable_size_usd=executable_size_usd,
            gross_cost_usd=gross_cost_usd,
            gross_payout_usd=gross_payout_usd,
            estimated_fees_usd=estimated_fees_usd,
            estimated_slippage_usd=estimated_slippage_usd,
            estimated_net_edge_usd=estimated_net_edge_usd,
            fill_completion_ratio=fill_completion_ratio,
            simulation_reason=SIMULATION_REASON_INSUFFICIENT_EDGE,
            raw_context=raw_context,
        )

    if executable_size_usd < normalized_intended_size:
        return ExecutionSimulationResult(
            simulation_status=SIMULATION_STATUS_PARTIALLY_EXECUTABLE,
            intended_size_usd=normalized_intended_size,
            executable_size_usd=executable_size_usd,
            gross_cost_usd=gross_cost_usd,
            gross_payout_usd=gross_payout_usd,
            estimated_fees_usd=estimated_fees_usd,
            estimated_slippage_usd=estimated_slippage_usd,
            estimated_net_edge_usd=estimated_net_edge_usd,
            fill_completion_ratio=fill_completion_ratio,
            simulation_reason=SIMULATION_REASON_PARTIALLY_EXECUTABLE,
            raw_context=raw_context,
        )

    return ExecutionSimulationResult(
        simulation_status=SIMULATION_STATUS_EXECUTABLE,
        intended_size_usd=normalized_intended_size,
        executable_size_usd=executable_size_usd,
        gross_cost_usd=gross_cost_usd,
        gross_payout_usd=gross_payout_usd,
        estimated_fees_usd=estimated_fees_usd,
        estimated_slippage_usd=estimated_slippage_usd,
        estimated_net_edge_usd=estimated_net_edge_usd,
        fill_completion_ratio=fill_completion_ratio,
        simulation_reason=SIMULATION_REASON_EXECUTABLE,
        raw_context=raw_context,
    )


def _rejected_result(
    *,
    intended_size_usd: Decimal,
    simulation_reason: str,
    raw_context: dict[str, Any],
) -> ExecutionSimulationResult:
    return ExecutionSimulationResult(
        simulation_status=SIMULATION_STATUS_REJECTED,
        intended_size_usd=intended_size_usd,
        executable_size_usd=ZERO,
        gross_cost_usd=ZERO,
        gross_payout_usd=ZERO,
        estimated_fees_usd=ZERO,
        estimated_slippage_usd=ZERO,
        estimated_net_edge_usd=ZERO,
        fill_completion_ratio=ZERO,
        simulation_reason=simulation_reason,
        raw_context=raw_context,
    )


def _max_bundle_size_from_snapshot(snapshot: SimulationSnapshotInput) -> Decimal:
    if snapshot.best_ask is None or snapshot.ask_depth_usd is None:
        return ZERO
    if snapshot.best_ask <= ZERO or snapshot.ask_depth_usd <= ZERO:
        return ZERO
    return _quantize_money(snapshot.ask_depth_usd / snapshot.best_ask)


def _normalize_intended_size(value: Decimal) -> Decimal:
    try:
        normalized = Decimal(value)
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("intended_size_usd must be a valid decimal amount") from exc
    if normalized <= ZERO:
        raise ValueError("intended_size_usd must be greater than zero")
    return _quantize_money(normalized)


def _positive_or_zero(value: Decimal | None) -> Decimal:
    if value is None or value <= ZERO:
        return ZERO
    return _quantize_money(value)


def _decimal_to_string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(_quantize_money(value), "f")


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_PRECISION)


def _quantize_ratio(value: Decimal) -> Decimal:
    return value.quantize(RATIO_PRECISION)
