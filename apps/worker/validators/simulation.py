from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from apps.worker.validators.executable_edge import (
    SUPPORTED_OPPORTUNITY_TYPE,
    ExecutableEdgeValidationResult,
    ExecutableMarketSnapshotInput,
    OrderBookLevelInput,
)


SIMULATION_VERSION = "simulation_validation_v1"
VALIDATION_TYPE = "simulation_validation"
STATUS_VALID = "valid"
STATUS_INVALID = "invalid"
STATUS_INCONCLUSIVE = "inconclusive"
RISK_FLAG_NONE = "none"
RISK_FLAG_PARTIAL_FILL = "partial_fill_risk"
RISK_FLAG_INSUFFICIENT_DEPTH = "insufficient_depth"
DEFAULT_REVIEW_NOTIONAL_USD = Decimal("100.0000")
SECONDS_PER_LEVEL_CONSUMED = 5
SECONDS_PER_LEG_SWITCH = 15
MONEY_PRECISION = Decimal("0.0001")
RATIO_PRECISION = Decimal("0.0001")
ZERO = Decimal("0.0000")
ONE = Decimal("1.0000")
BUNDLE_PAYOUT_USD = Decimal("1.0000")


@dataclass(frozen=True, slots=True)
class SimulationValidationInput:
    opportunity_id: int
    event_group_key: str
    involved_market_ids: list[int]
    family: str | None
    opportunity_type: str


@dataclass(frozen=True, slots=True)
class SimulationValidationResult:
    validation_type: str
    status: str
    score: Decimal | None
    summary: str
    details: dict[str, Any]
    simulation_version: str
    fill_completion_ratio: Decimal
    execution_feasible: bool
    capital_lock_estimate_hours: Decimal | None
    execution_risk_flag: str
    intended_size_usd: Decimal
    executable_size_usd: Decimal
    executable_edge: Decimal | None
    fee_cost_usd: Decimal
    slippage_cost_usd: Decimal
    capital_required_usd: Decimal
    execution_time_sensitivity_seconds: int


@dataclass(frozen=True, slots=True)
class _ConsumedLevel:
    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class _LegFill:
    requested_size: Decimal
    filled_size: Decimal
    gross_cost_usd: Decimal
    fee_cost_usd: Decimal
    slippage_cost_usd: Decimal
    consumed_levels: tuple[_ConsumedLevel, ...]


def validate_simulation_execution(
    candidate: SimulationValidationInput,
    *,
    execution_result: ExecutableEdgeValidationResult,
    market_snapshots: dict[int, ExecutableMarketSnapshotInput | None],
) -> SimulationValidationResult:
    base_details = {
        "simulation_version": SIMULATION_VERSION,
        "simulation_basis": "deterministic_sequential_yes_ask_consumption",
        "sequencing_policy": "opportunity_market_order",
        "capital_model": "peak_cumulative_leg_cost_plus_fees_during_sequential_execution",
        "time_model": {
            "seconds_per_level_consumed": SECONDS_PER_LEVEL_CONSUMED,
            "seconds_per_leg_switch": SECONDS_PER_LEG_SWITCH,
        },
        "opportunity_type": candidate.opportunity_type,
        "family": candidate.family,
        "execution_validation_reference": {
            "validation_type": execution_result.validation_type,
            "status": execution_result.status,
            "reason_code": execution_result.reason_code,
            "validator_version": execution_result.validator_version,
        },
    }

    if candidate.opportunity_type != SUPPORTED_OPPORTUNITY_TYPE:
        return _build_result(
            status=STATUS_INCONCLUSIVE,
            summary="simulation validation is inconclusive because this payout contract is not yet supported",
            details={
                **base_details,
                "reason_code": "unsupported_payout_contract",
                "supported_contract_type": SUPPORTED_OPPORTUNITY_TYPE,
            },
            fill_completion_ratio=ZERO,
            execution_feasible=False,
            execution_risk_flag=RISK_FLAG_INSUFFICIENT_DEPTH,
            intended_size_usd=ZERO,
            executable_size_usd=ZERO,
            fee_cost_usd=ZERO,
            slippage_cost_usd=ZERO,
            capital_required_usd=ZERO,
            execution_time_sensitivity_seconds=0,
        )

    if execution_result.status != "valid":
        return _build_result(
            status=STATUS_INVALID,
            summary="simulation validation rejected the opportunity because executable-edge validation did not pass",
            details={
                **base_details,
                "reason_code": "execution_validation_prerequisite_failed",
            },
            fill_completion_ratio=ZERO,
            execution_feasible=False,
            execution_risk_flag=RISK_FLAG_INSUFFICIENT_DEPTH,
            intended_size_usd=ZERO,
            executable_size_usd=ZERO,
            fee_cost_usd=ZERO,
            slippage_cost_usd=ZERO,
            capital_required_usd=ZERO,
            execution_time_sensitivity_seconds=0,
        )

    missing_market_ids = [
        market_id
        for market_id in candidate.involved_market_ids
        if market_snapshots.get(market_id) is None
    ]
    if missing_market_ids:
        return _build_result(
            status=STATUS_INVALID,
            summary="simulation validation rejected the opportunity because required order-book snapshots are missing",
            details={
                **base_details,
                "reason_code": "missing_snapshot",
                "missing_market_ids": missing_market_ids,
            },
            fill_completion_ratio=ZERO,
            execution_feasible=False,
            execution_risk_flag=RISK_FLAG_INSUFFICIENT_DEPTH,
            intended_size_usd=ZERO,
            executable_size_usd=ZERO,
            fee_cost_usd=ZERO,
            slippage_cost_usd=ZERO,
            capital_required_usd=ZERO,
            execution_time_sensitivity_seconds=0,
        )

    snapshots = [market_snapshots[market_id] for market_id in candidate.involved_market_ids]
    ordered_snapshots = [snapshot for snapshot in snapshots if snapshot is not None]
    invalid_books = [snapshot.market_id for snapshot in ordered_snapshots if not snapshot.ask_levels]
    if invalid_books:
        return _build_result(
            status=STATUS_INVALID,
            summary="simulation validation rejected the opportunity because yes-side ask depth is unavailable",
            details={
                **base_details,
                "reason_code": "missing_yes_ask_levels",
                "missing_yes_ask_level_market_ids": invalid_books,
            },
            fill_completion_ratio=ZERO,
            execution_feasible=False,
            execution_risk_flag=RISK_FLAG_INSUFFICIENT_DEPTH,
            intended_size_usd=ZERO,
            executable_size_usd=ZERO,
            fee_cost_usd=ZERO,
            slippage_cost_usd=ZERO,
            capital_required_usd=ZERO,
            execution_time_sensitivity_seconds=0,
        )

    intended_size_usd = _derive_intended_size(execution_result)
    current_target_size = intended_size_usd
    leg_fills: list[_LegFill] = []
    peak_capital_required = ZERO
    cumulative_capital_required = ZERO
    execution_time_sensitivity_seconds = 0

    for index, snapshot in enumerate(ordered_snapshots):
        if index > 0:
            execution_time_sensitivity_seconds += SECONDS_PER_LEG_SWITCH
        if current_target_size <= ZERO:
            break

        leg_fill = _simulate_leg_fill(snapshot=snapshot, size=current_target_size)
        leg_fills.append(leg_fill)
        cumulative_capital_required = _quantize_money(
            cumulative_capital_required + leg_fill.gross_cost_usd + leg_fill.fee_cost_usd
        )
        peak_capital_required = max(peak_capital_required, cumulative_capital_required)
        execution_time_sensitivity_seconds += len(leg_fill.consumed_levels) * SECONDS_PER_LEVEL_CONSUMED

        if leg_fill.filled_size < current_target_size:
            current_target_size = leg_fill.filled_size

    if len(leg_fills) < len(ordered_snapshots):
        current_target_size = ZERO

    executable_size_usd = _quantize_money(current_target_size)
    fill_completion_ratio = (
        _quantize_ratio(executable_size_usd / intended_size_usd)
        if intended_size_usd > ZERO
        else ZERO
    )
    execution_feasible = executable_size_usd == intended_size_usd and len(leg_fills) == len(ordered_snapshots)
    execution_risk_flag = _derive_execution_risk_flag(fill_completion_ratio)
    capital_lock_estimate_hours = _quantize_money(
        Decimal(execution_time_sensitivity_seconds) / Decimal("3600")
    )

    matched_leg_fills = [
        _simulate_leg_fill(snapshot=snapshot, size=executable_size_usd)
        for snapshot in ordered_snapshots
        if executable_size_usd > ZERO
    ]
    matched_fee_cost_usd = _quantize_money(sum((fill.fee_cost_usd for fill in matched_leg_fills), ZERO))
    matched_slippage_cost_usd = _quantize_money(sum((fill.slippage_cost_usd for fill in matched_leg_fills), ZERO))
    matched_gross_cost_usd = _quantize_money(sum((fill.gross_cost_usd for fill in matched_leg_fills), ZERO))
    executable_edge = None
    if execution_feasible and executable_size_usd > ZERO:
        executable_edge = _quantize_money(
            (
                executable_size_usd * BUNDLE_PAYOUT_USD
                - matched_gross_cost_usd
                - matched_fee_cost_usd
            )
            / executable_size_usd
        )

    details = {
        **base_details,
        "reason_code": (
            "full_sequential_execution_verified"
            if execution_feasible
            else ("partial_fill_detected" if executable_size_usd > ZERO else "insufficient_depth")
        ),
        "intended_size_usd": format(intended_size_usd, "f"),
        "intended_size_source": "max(standard_review_notional_usd, executable_edge_min_executable_size)",
        "standard_review_notional_usd": format(DEFAULT_REVIEW_NOTIONAL_USD, "f"),
        "execution_validation_max_positive_size": (
            format(execution_result.max_positive_size, "f")
            if execution_result.max_positive_size is not None
            else None
        ),
        "execution_validation_min_executable_size": (
            format(execution_result.min_executable_size, "f")
            if execution_result.min_executable_size is not None
            else None
        ),
        "execution_feasible": execution_feasible,
        "fill_completion_ratio": format(fill_completion_ratio, "f"),
        "execution_risk_flag": execution_risk_flag,
        "execution_time_sensitivity_seconds": execution_time_sensitivity_seconds,
        "capital_required_usd": format(peak_capital_required, "f"),
        "capital_lock_estimate_hours": format(capital_lock_estimate_hours, "f"),
        "legs": [
            _serialize_leg(
                market_id=snapshot.market_id,
                attempted_fill=attempted_fill,
                matched_fill=(
                    matched_leg_fills[index]
                    if index < len(matched_leg_fills)
                    else None
                ),
                final_executable_size=executable_size_usd,
            )
            for index, (snapshot, attempted_fill) in enumerate(zip(ordered_snapshots, leg_fills, strict=False))
        ],
    }

    if execution_feasible:
        return _build_result(
            status=STATUS_VALID,
            score=ONE,
            summary="simulation validation verified full sequential execution at the deterministic review size",
            details=details,
            fill_completion_ratio=fill_completion_ratio,
            execution_feasible=True,
            execution_risk_flag=RISK_FLAG_NONE,
            intended_size_usd=intended_size_usd,
            executable_size_usd=executable_size_usd,
            executable_edge=executable_edge,
            fee_cost_usd=matched_fee_cost_usd,
            slippage_cost_usd=matched_slippage_cost_usd,
            capital_required_usd=peak_capital_required,
            capital_lock_estimate_hours=capital_lock_estimate_hours,
            execution_time_sensitivity_seconds=execution_time_sensitivity_seconds,
        )

    return _build_result(
        status=STATUS_INVALID,
        score=fill_completion_ratio,
        summary=(
            "simulation validation detected partial-fill risk under sequential execution"
            if executable_size_usd > ZERO
            else "simulation validation rejected the opportunity because sequential execution cannot complete"
        ),
        details=details,
        fill_completion_ratio=fill_completion_ratio,
        execution_feasible=False,
        execution_risk_flag=execution_risk_flag,
        intended_size_usd=intended_size_usd,
        executable_size_usd=executable_size_usd,
        fee_cost_usd=matched_fee_cost_usd,
        slippage_cost_usd=matched_slippage_cost_usd,
        capital_required_usd=peak_capital_required,
        capital_lock_estimate_hours=capital_lock_estimate_hours,
        execution_time_sensitivity_seconds=execution_time_sensitivity_seconds,
    )


def _derive_intended_size(execution_result: ExecutableEdgeValidationResult) -> Decimal:
    minimum_size = execution_result.min_executable_size or ZERO
    return _quantize_money(max(DEFAULT_REVIEW_NOTIONAL_USD, minimum_size))


def _simulate_leg_fill(
    *,
    snapshot: ExecutableMarketSnapshotInput,
    size: Decimal,
) -> _LegFill:
    normalized_size = _quantize_money(size)
    remaining = normalized_size
    filled = ZERO
    gross_cost_usd = ZERO
    consumed: list[_ConsumedLevel] = []

    for level in snapshot.ask_levels:
        if remaining <= ZERO:
            break
        if level.size <= ZERO:
            continue
        take_size = min(level.size, remaining)
        if take_size <= ZERO:
            continue
        take_size = _quantize_money(take_size)
        filled = _quantize_money(filled + take_size)
        gross_cost_usd = _quantize_money(gross_cost_usd + (take_size * level.price))
        consumed.append(_ConsumedLevel(price=level.price, size=take_size))
        remaining = _quantize_money(remaining - take_size)

    fee_cost_usd = _fee_for_fill(consumed_levels=tuple(consumed), taker_fee_bps=snapshot.taker_fee_bps)
    top_price = snapshot.ask_levels[0].price if snapshot.ask_levels else ZERO
    slippage_cost_usd = _quantize_money(gross_cost_usd - _quantize_money(filled * top_price))
    return _LegFill(
        requested_size=normalized_size,
        filled_size=_quantize_money(filled),
        gross_cost_usd=_quantize_money(gross_cost_usd),
        fee_cost_usd=fee_cost_usd,
        slippage_cost_usd=slippage_cost_usd,
        consumed_levels=tuple(consumed),
    )


def _serialize_leg(
    *,
    market_id: int,
    attempted_fill: _LegFill,
    matched_fill: _LegFill | None,
    final_executable_size: Decimal,
) -> dict[str, Any]:
    matched_gross_cost = matched_fill.gross_cost_usd if matched_fill is not None else ZERO
    matched_fee_cost = matched_fill.fee_cost_usd if matched_fill is not None else ZERO
    matched_slippage_cost = matched_fill.slippage_cost_usd if matched_fill is not None else ZERO
    exposed_size = _quantize_money(max(attempted_fill.filled_size - final_executable_size, ZERO))
    exposed_cost = _quantize_money(max(attempted_fill.gross_cost_usd - matched_gross_cost, ZERO))
    return {
        "market_id": market_id,
        "requested_size": format(attempted_fill.requested_size, "f"),
        "filled_size": format(attempted_fill.filled_size, "f"),
        "matched_bundle_size": format(final_executable_size, "f"),
        "gross_cost_usd": format(attempted_fill.gross_cost_usd, "f"),
        "fee_cost_usd": format(attempted_fill.fee_cost_usd, "f"),
        "slippage_cost_usd": format(attempted_fill.slippage_cost_usd, "f"),
        "matched_gross_cost_usd": format(matched_gross_cost, "f"),
        "matched_fee_cost_usd": format(matched_fee_cost, "f"),
        "matched_slippage_cost_usd": format(matched_slippage_cost, "f"),
        "exposed_unmatched_size": format(exposed_size, "f"),
        "exposed_unmatched_cost_usd": format(exposed_cost, "f"),
        "consumed_levels": [
            {
                "price": format(level.price, "f"),
                "size": format(level.size, "f"),
            }
            for level in attempted_fill.consumed_levels
        ],
    }


def _derive_execution_risk_flag(fill_completion_ratio: Decimal) -> str:
    if fill_completion_ratio >= ONE:
        return RISK_FLAG_NONE
    if fill_completion_ratio > ZERO:
        return RISK_FLAG_PARTIAL_FILL
    return RISK_FLAG_INSUFFICIENT_DEPTH


def _fee_for_fill(
    *,
    consumed_levels: tuple[_ConsumedLevel, ...],
    taker_fee_bps: Decimal,
) -> Decimal:
    if taker_fee_bps <= ZERO:
        return ZERO

    fee_rate = taker_fee_bps / Decimal("10000")
    total_fee = ZERO
    for level in consumed_levels:
        total_fee += level.size * fee_rate * level.price * (Decimal("1") - level.price)
    return _quantize_money(total_fee)


def _build_result(
    *,
    status: str,
    summary: str,
    details: dict[str, Any],
    fill_completion_ratio: Decimal,
    execution_feasible: bool,
    execution_risk_flag: str,
    intended_size_usd: Decimal,
    executable_size_usd: Decimal,
    fee_cost_usd: Decimal,
    slippage_cost_usd: Decimal,
    capital_required_usd: Decimal,
    execution_time_sensitivity_seconds: int,
    score: Decimal | None = None,
    executable_edge: Decimal | None = None,
    capital_lock_estimate_hours: Decimal | None = None,
) -> SimulationValidationResult:
    return SimulationValidationResult(
        validation_type=VALIDATION_TYPE,
        status=status,
        score=score,
        summary=summary,
        details=details,
        simulation_version=SIMULATION_VERSION,
        fill_completion_ratio=fill_completion_ratio,
        execution_feasible=execution_feasible,
        capital_lock_estimate_hours=capital_lock_estimate_hours,
        execution_risk_flag=execution_risk_flag,
        intended_size_usd=intended_size_usd,
        executable_size_usd=executable_size_usd,
        executable_edge=executable_edge,
        fee_cost_usd=fee_cost_usd,
        slippage_cost_usd=slippage_cost_usd,
        capital_required_usd=capital_required_usd,
        execution_time_sensitivity_seconds=execution_time_sensitivity_seconds,
    )


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_PRECISION, rounding=ROUND_HALF_UP)


def _quantize_ratio(value: Decimal) -> Decimal:
    return value.quantize(RATIO_PRECISION, rounding=ROUND_HALF_UP)
