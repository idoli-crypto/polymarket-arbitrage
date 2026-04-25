from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from apps.worker.validators.executable_edge import (
    SUPPORTED_OPPORTUNITY_TYPE,
    VALIDATOR_VERSION as EXECUTION_VALIDATOR_VERSION,
    ExecutableEdgeValidationInput,
    evaluate_execution_at_size,
    parse_executable_market_snapshot,
    validate_executable_edge,
)


SIMULATION_VERSION = "execution_sim_v2"
DEFAULT_INTENDED_SIZE_USD = Decimal("100.0000")
SIMULATION_STATUS_EXECUTABLE = "executable"
SIMULATION_STATUS_PARTIALLY_EXECUTABLE = "partially_executable"
SIMULATION_STATUS_REJECTED = "rejected"
SIMULATION_REASON_EXECUTABLE = "executable"
SIMULATION_REASON_PARTIALLY_EXECUTABLE = "partially_executable"
SIMULATION_REASON_INSUFFICIENT_DEPTH = "insufficient_depth"
SIMULATION_REASON_MISSING_SNAPSHOT = "missing_snapshot"
SIMULATION_REASON_MISSING_ORDER_BOOK = "missing_order_book"
SIMULATION_REASON_MISSING_FEE_RATE = "missing_taker_fee_rate"
SIMULATION_REASON_BELOW_MIN_ORDER_SIZE = "below_min_order_size"
SIMULATION_REASON_INSUFFICIENT_EDGE = "insufficient_edge"
SIMULATION_REASON_UNSUPPORTED_OPPORTUNITY_TYPE = "unsupported_opportunity_type"
MONEY_PRECISION = Decimal("0.0001")
RATIO_PRECISION = Decimal("0.0001")
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
    order_book_json: dict[str, Any] | list[Any] | None = None
    raw_market_json: dict[str, Any] | list[Any] | None = None


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
        "validation_source": EXECUTION_VALIDATOR_VERSION,
        "size_basis": "bundle_payout_notional_usd",
        "pricing_basis": "persisted_yes_ask_order_book_levels",
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

    executable_snapshots = [
        parse_executable_market_snapshot(
            market_id=snapshot.market_id,
            snapshot_id=snapshot.snapshot_id,
            captured_at=snapshot.captured_at,
            order_book_json=snapshot.order_book_json,
            raw_market_json=snapshot.raw_market_json,
        )
        for market_id in opportunity.involved_market_ids
        if (snapshot := latest_snapshots.get(market_id)) is not None
    ]

    validation_result = validate_executable_edge(
        ExecutableEdgeValidationInput(
            opportunity_id=opportunity.opportunity_id,
            event_group_key=opportunity.event_group_key,
            involved_market_ids=list(opportunity.involved_market_ids),
            family=None,
            opportunity_type=opportunity.opportunity_type,
        ),
        market_snapshots={snapshot.market_id: snapshot for snapshot in executable_snapshots},
        reference_time=max(snapshot.captured_at for snapshot in executable_snapshots),
    )

    selected_snapshot_ids = {
        str(snapshot.market_id): snapshot.snapshot_id
        for snapshot in executable_snapshots
    }
    raw_context = {
        **base_context,
        "selected_snapshot_ids": selected_snapshot_ids,
        "execution_validation": validation_result.details,
        "execution_validation_status": validation_result.status,
        "execution_validation_reason": validation_result.reason_code,
    }

    if validation_result.reason_code == "missing_yes_ask_levels":
        return _rejected_result(
            intended_size_usd=normalized_intended_size,
            simulation_reason=SIMULATION_REASON_MISSING_ORDER_BOOK,
            raw_context=raw_context,
        )
    if validation_result.reason_code == "missing_taker_fee_rate":
        return _rejected_result(
            intended_size_usd=normalized_intended_size,
            simulation_reason=SIMULATION_REASON_MISSING_FEE_RATE,
            raw_context=raw_context,
        )
    if validation_result.reason_code in {"missing_snapshot", "insufficient_depth"}:
        return _rejected_result(
            intended_size_usd=normalized_intended_size,
            simulation_reason=SIMULATION_REASON_INSUFFICIENT_DEPTH,
            raw_context=raw_context,
        )
    if validation_result.reason_code == "non_positive_fee_adjusted_edge":
        return _rejected_result(
            intended_size_usd=normalized_intended_size,
            simulation_reason=SIMULATION_REASON_INSUFFICIENT_EDGE,
            raw_context=raw_context,
        )

    min_executable_size = validation_result.min_executable_size
    max_positive_size = validation_result.max_positive_size
    if min_executable_size is not None and normalized_intended_size < min_executable_size:
        return _rejected_result(
            intended_size_usd=normalized_intended_size,
            simulation_reason=SIMULATION_REASON_BELOW_MIN_ORDER_SIZE,
            raw_context={
                **raw_context,
                "minimum_executable_size": format(min_executable_size, "f"),
            },
        )

    if max_positive_size is None or max_positive_size <= ZERO:
        return _rejected_result(
            intended_size_usd=normalized_intended_size,
            simulation_reason=SIMULATION_REASON_INSUFFICIENT_EDGE,
            raw_context=raw_context,
        )

    executable_size_usd = _quantize_money(min(normalized_intended_size, max_positive_size))
    execution_point = evaluate_execution_at_size(size=executable_size_usd, market_snapshots=executable_snapshots)
    if execution_point is None:
        return _rejected_result(
            intended_size_usd=normalized_intended_size,
            simulation_reason=SIMULATION_REASON_INSUFFICIENT_DEPTH,
            raw_context=raw_context,
        )

    gross_payout_usd = executable_size_usd
    estimated_net_edge_usd = _quantize_money(
        gross_payout_usd - execution_point.gross_cost_usd - execution_point.fee_cost_usd
    )
    fill_completion_ratio = _quantize_ratio(executable_size_usd / normalized_intended_size)

    result_context = {
        **raw_context,
        "executed_size_usd": format(executable_size_usd, "f"),
        "gross_edge": format(execution_point.gross_edge, "f"),
        "fee_adjusted_edge": format(execution_point.fee_adjusted_edge, "f"),
        "slippage_cost_usd": format(execution_point.slippage_cost_usd, "f"),
        "fee_cost_usd": format(execution_point.fee_cost_usd, "f"),
    }

    if executable_size_usd < normalized_intended_size:
        return ExecutionSimulationResult(
            simulation_status=SIMULATION_STATUS_PARTIALLY_EXECUTABLE,
            intended_size_usd=normalized_intended_size,
            executable_size_usd=executable_size_usd,
            gross_cost_usd=execution_point.gross_cost_usd,
            gross_payout_usd=gross_payout_usd,
            estimated_fees_usd=execution_point.fee_cost_usd,
            estimated_slippage_usd=execution_point.slippage_cost_usd,
            estimated_net_edge_usd=estimated_net_edge_usd,
            fill_completion_ratio=fill_completion_ratio,
            simulation_reason=SIMULATION_REASON_PARTIALLY_EXECUTABLE,
            raw_context=result_context,
        )

    return ExecutionSimulationResult(
        simulation_status=SIMULATION_STATUS_EXECUTABLE,
        intended_size_usd=normalized_intended_size,
        executable_size_usd=executable_size_usd,
        gross_cost_usd=execution_point.gross_cost_usd,
        gross_payout_usd=gross_payout_usd,
        estimated_fees_usd=execution_point.fee_cost_usd,
        estimated_slippage_usd=execution_point.slippage_cost_usd,
        estimated_net_edge_usd=estimated_net_edge_usd,
        fill_completion_ratio=fill_completion_ratio,
        simulation_reason=SIMULATION_REASON_EXECUTABLE,
        raw_context=result_context,
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


def _normalize_intended_size(value: Decimal) -> Decimal:
    try:
        normalized = Decimal(value)
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("intended_size_usd must be a valid decimal amount") from exc
    if normalized <= ZERO:
        raise ValueError("intended_size_usd must be greater than zero")
    return _quantize_money(normalized)


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_PRECISION)


def _quantize_ratio(value: Decimal) -> Decimal:
    return value.quantize(RATIO_PRECISION)
