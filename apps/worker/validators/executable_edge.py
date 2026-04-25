from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any


VALIDATOR_VERSION = "executable_edge_validation_v1"
VALIDATION_TYPE = "executable_edge_validation"
STATUS_VALID = "valid"
STATUS_INVALID = "invalid"
STATUS_INCONCLUSIVE = "inconclusive"
# PR 7 only defines an executable payout contract for Neg Risk long-YES bundles.
# Other families stay inconclusive until they get an explicit payout contract.
SUPPORTED_OPPORTUNITY_TYPE = "neg_risk_long_yes_bundle"
SUPPORTED_CONTRACT_TYPE = "neg_risk_long_yes_bundle"
SNAPSHOT_FRESHNESS_THRESHOLD = timedelta(seconds=30)
MONEY_PRECISION = Decimal("0.0001")
EDGE_PRECISION = Decimal("0.0001")
RATIO_PRECISION = Decimal("0.0001")
FEE_PRECISION = Decimal("0.00001")
ZERO = Decimal("0")
BUNDLE_PAYOUT_USD = Decimal("1.0000")
SUGGESTED_BUCKETS: tuple[tuple[Decimal, Decimal, str], ...] = (
    (Decimal("0"), Decimal("10"), "0-10"),
    (Decimal("10"), Decimal("25"), "10-25"),
    (Decimal("25"), Decimal("50"), "25-50"),
    (Decimal("50"), Decimal("100"), "50-100"),
    (Decimal("100"), Decimal("250"), "100-250"),
    (Decimal("250"), Decimal("500"), "250-500"),
    (Decimal("500"), Decimal("999999999"), "500+"),
)


@dataclass(frozen=True, slots=True)
class OrderBookLevelInput:
    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class ExecutableMarketSnapshotInput:
    market_id: int
    snapshot_id: int
    captured_at: datetime
    ask_levels: tuple[OrderBookLevelInput, ...]
    taker_fee_bps: Decimal
    min_order_size: Decimal
    raw_market_json: dict[str, Any] | None
    order_book_json: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class ExecutableEdgeValidationInput:
    opportunity_id: int
    event_group_key: str
    involved_market_ids: list[int]
    family: str | None
    opportunity_type: str


@dataclass(frozen=True, slots=True)
class ExecutionPoint:
    size: Decimal
    gross_cost_usd: Decimal
    gross_edge: Decimal
    fee_cost_usd: Decimal
    fee_adjusted_edge: Decimal
    slippage_cost_usd: Decimal


@dataclass(frozen=True, slots=True)
class ExecutableEdgeValidationResult:
    validation_type: str
    status: str
    score: Decimal | None
    summary: str
    details: dict[str, Any]
    validator_version: str
    reason_code: str
    top_of_book_edge: Decimal | None
    depth_weighted_edge: Decimal | None
    fee_adjusted_edge: Decimal | None
    min_executable_size: Decimal | None
    suggested_notional_bucket: str | None
    execution_size: Decimal | None
    max_positive_size: Decimal | None
    gross_cost_usd: Decimal | None
    gross_payout_usd: Decimal | None
    fee_cost_usd: Decimal | None
    slippage_cost_usd: Decimal | None


def validate_executable_edge(
    candidate: ExecutableEdgeValidationInput,
    *,
    market_snapshots: dict[int, ExecutableMarketSnapshotInput | None],
    reference_time: datetime,
) -> ExecutableEdgeValidationResult:
    base_details = {
        "validator_version": VALIDATOR_VERSION,
        "pricing_basis": "persisted_yes_ask_order_book_levels",
        "size_basis": "bundle_payout_notional_usd",
        "fee_formula": "shares * fee_rate * price * (1-price)",
        "slippage_basis": "depth_weighted_cost_minus_top_of_book_cost",
        "supported_contract_type": SUPPORTED_CONTRACT_TYPE,
        "opportunity_type": candidate.opportunity_type,
        "family": candidate.family,
    }

    if candidate.opportunity_type != SUPPORTED_OPPORTUNITY_TYPE:
        details = {
            **base_details,
            "unsupported_payout_contract": candidate.opportunity_type,
            "fee_source": "persisted_market_metadata_only",
        }
        return _build_result(
            status=STATUS_INCONCLUSIVE,
            reason_code="unsupported_payout_contract",
            summary="execution validation is inconclusive because this payout contract is not yet supported",
            details=details,
        )

    missing_market_ids = [
        market_id
        for market_id in candidate.involved_market_ids
        if market_snapshots.get(market_id) is None
    ]
    if missing_market_ids:
        return _build_result(
            status=STATUS_INVALID,
            reason_code="missing_snapshot",
            summary="execution validation failed because a required snapshot is missing",
            details={
                **base_details,
                "missing_market_ids": missing_market_ids,
                "fee_source": "persisted_market_metadata_only",
            },
        )

    ordered_markets = [market_snapshots[market_id] for market_id in candidate.involved_market_ids]
    assert all(snapshot is not None for snapshot in ordered_markets)
    snapshots = [snapshot for snapshot in ordered_markets if snapshot is not None]
    snapshot_audit = _market_audit_payload(snapshots, reference_time=reference_time)

    stale_market_ids = [
        market["market_id"]
        for market in snapshot_audit
        if market["snapshot_age_seconds"] is None
        or Decimal(market["snapshot_age_seconds"])
        > Decimal(str(int(SNAPSHOT_FRESHNESS_THRESHOLD.total_seconds())))
    ]
    if stale_market_ids:
        return _build_result(
            status=STATUS_INCONCLUSIVE,
            reason_code="stale_order_book_snapshot",
            summary="execution validation is inconclusive because the persisted order book snapshot is stale",
            details={
                **base_details,
                "stale_market_ids": stale_market_ids,
                "snapshot_freshness_seconds": int(SNAPSHOT_FRESHNESS_THRESHOLD.total_seconds()),
                "markets": snapshot_audit,
                "fee_source": "persisted_market_metadata_only",
            },
        )

    invalid_books = [snapshot.market_id for snapshot in snapshots if not snapshot.ask_levels]
    if invalid_books:
        return _build_result(
            status=STATUS_INVALID,
            reason_code="missing_yes_ask_levels",
            summary="execution validation failed because persisted yes-side ask levels are unavailable",
            details={
                **base_details,
                "missing_yes_ask_level_market_ids": invalid_books,
                "markets": snapshot_audit,
                "fee_source": "persisted_market_metadata_only",
            },
        )

    fee_rate_missing = [
        snapshot.market_id
        for snapshot in snapshots
        if snapshot.taker_fee_bps < ZERO
    ]
    if fee_rate_missing:
        return _build_result(
            status=STATUS_INVALID,
            reason_code="missing_taker_fee_rate",
            summary="execution validation failed because taker fee metadata is unavailable",
            details={
                **base_details,
                "missing_fee_market_ids": fee_rate_missing,
                "markets": snapshot_audit,
                "fee_source": "persisted_market_metadata_only",
            },
        )

    top_of_book_sum = ZERO
    top_of_book_cost_per_bundle = ZERO
    max_feasible_size = None
    minimum_order_floor = ZERO

    for snapshot in snapshots:
        top_level = snapshot.ask_levels[0]
        top_of_book_sum += top_level.price
        top_of_book_cost_per_bundle += top_level.price
        total_depth = _quantize_money(sum(level.size for level in snapshot.ask_levels))
        max_feasible_size = total_depth if max_feasible_size is None else min(max_feasible_size, total_depth)
        minimum_order_floor = max(minimum_order_floor, snapshot.min_order_size)

    assert max_feasible_size is not None
    top_of_book_edge = _quantize_edge(BUNDLE_PAYOUT_USD - top_of_book_sum)
    candidate_sizes = _candidate_sizes(snapshots, minimum_order_floor, max_feasible_size)
    if not candidate_sizes:
        return _build_result(
            status=STATUS_INVALID,
            reason_code="insufficient_depth",
            summary="execution validation failed because executable depth is zero",
            details={
                **base_details,
                "markets": snapshot_audit,
                "max_feasible_size": format(_quantize_money(max_feasible_size), "f"),
                "minimum_order_floor": format(_quantize_money(minimum_order_floor), "f"),
                "fee_source": "persisted_market_metadata_only",
            },
            top_of_book_edge=top_of_book_edge,
        )

    execution_points: list[ExecutionPoint] = []
    for size in candidate_sizes:
        point = _evaluate_size(size, snapshots, top_of_book_cost_per_bundle)
        execution_points.append(point)

    positive_points = [point for point in execution_points if point.fee_adjusted_edge > ZERO]
    analysis_point = positive_points[-1] if positive_points else execution_points[-1]
    min_positive_point = positive_points[0] if positive_points else None

    details = {
        **base_details,
        "markets": snapshot_audit,
        "minimum_order_floor": format(_quantize_money(minimum_order_floor), "f"),
        "max_feasible_size": format(_quantize_money(max_feasible_size), "f"),
        "analysis_size": format(analysis_point.size, "f"),
        "snapshot_freshness_seconds": int(SNAPSHOT_FRESHNESS_THRESHOLD.total_seconds()),
        "fee_source": "persisted_market_metadata_only",
        "checkpoints": [
            {
                "size": format(point.size, "f"),
                "gross_cost_usd": format(point.gross_cost_usd, "f"),
                "gross_edge": format(point.gross_edge, "f"),
                "fee_cost_usd": format(point.fee_cost_usd, "f"),
                "fee_adjusted_edge": format(point.fee_adjusted_edge, "f"),
                "slippage_cost_usd": format(point.slippage_cost_usd, "f"),
            }
            for point in execution_points
        ],
        "selected_checkpoint": {
            "size": format(analysis_point.size, "f"),
            "gross_cost_usd": format(analysis_point.gross_cost_usd, "f"),
            "gross_edge": format(analysis_point.gross_edge, "f"),
            "fee_cost_usd": format(analysis_point.fee_cost_usd, "f"),
            "fee_adjusted_edge": format(analysis_point.fee_adjusted_edge, "f"),
            "slippage_cost_usd": format(analysis_point.slippage_cost_usd, "f"),
        },
        "top_of_book_edge": format(top_of_book_edge, "f"),
    }

    if not positive_points:
        return _build_result(
            status=STATUS_INVALID,
            reason_code="non_positive_fee_adjusted_edge",
            summary="execution validation rejected the opportunity because the fee-adjusted edge is not positive",
            details=details,
            top_of_book_edge=top_of_book_edge,
            depth_weighted_edge=analysis_point.gross_edge,
            fee_adjusted_edge=analysis_point.fee_adjusted_edge,
            execution_size=analysis_point.size,
            gross_cost_usd=analysis_point.gross_cost_usd,
            gross_payout_usd=_quantize_money(analysis_point.size * BUNDLE_PAYOUT_USD),
            fee_cost_usd=analysis_point.fee_cost_usd,
            slippage_cost_usd=analysis_point.slippage_cost_usd,
        )

    suggested_bucket = _suggested_notional_bucket(analysis_point.size)
    return _build_result(
        status=STATUS_VALID,
        reason_code="positive_fee_adjusted_edge_verified",
        summary="execution validation verified a positive fee-adjusted edge against persisted order-book depth",
        details=details,
        score=Decimal("1.0000"),
        top_of_book_edge=top_of_book_edge,
        depth_weighted_edge=analysis_point.gross_edge,
        fee_adjusted_edge=analysis_point.fee_adjusted_edge,
        min_executable_size=min_positive_point.size if min_positive_point is not None else None,
        suggested_notional_bucket=suggested_bucket,
        execution_size=analysis_point.size,
        max_positive_size=analysis_point.size,
        gross_cost_usd=analysis_point.gross_cost_usd,
        gross_payout_usd=_quantize_money(analysis_point.size * BUNDLE_PAYOUT_USD),
        fee_cost_usd=analysis_point.fee_cost_usd,
        slippage_cost_usd=analysis_point.slippage_cost_usd,
    )


def parse_executable_market_snapshot(
    *,
    market_id: int,
    snapshot_id: int,
    captured_at: datetime,
    order_book_json: dict[str, Any] | list[Any] | None,
    raw_market_json: dict[str, Any] | list[Any] | None,
) -> ExecutableMarketSnapshotInput:
    order_book_payload = order_book_json if isinstance(order_book_json, dict) else {}
    raw_market_payload = raw_market_json if isinstance(raw_market_json, dict) else {}
    ask_levels = tuple(_parse_yes_ask_levels(order_book_payload))
    taker_fee_bps = _extract_taker_fee_bps(raw_market_payload)
    min_order_size = _extract_min_order_size(raw_market_payload)
    return ExecutableMarketSnapshotInput(
        market_id=market_id,
        snapshot_id=snapshot_id,
        captured_at=captured_at,
        ask_levels=ask_levels,
        taker_fee_bps=taker_fee_bps,
        min_order_size=min_order_size,
        raw_market_json=raw_market_payload or None,
        order_book_json=order_book_payload or None,
    )


def evaluate_execution_at_size(
    *,
    size: Decimal,
    market_snapshots: list[ExecutableMarketSnapshotInput],
) -> ExecutionPoint | None:
    if size <= ZERO:
        return None
    if any(not snapshot.ask_levels for snapshot in market_snapshots):
        return None
    max_feasible = min(_quantize_money(sum(level.size for level in snapshot.ask_levels)) for snapshot in market_snapshots)
    if size > max_feasible:
        return None
    top_of_book_cost_per_bundle = sum(snapshot.ask_levels[0].price for snapshot in market_snapshots)
    return _evaluate_size(_quantize_money(size), market_snapshots, top_of_book_cost_per_bundle)


def _candidate_sizes(
    snapshots: list[ExecutableMarketSnapshotInput],
    minimum_order_floor: Decimal,
    max_feasible_size: Decimal,
) -> list[Decimal]:
    sizes: set[Decimal] = set()
    if minimum_order_floor > ZERO and minimum_order_floor <= max_feasible_size:
        sizes.add(_quantize_money(minimum_order_floor))

    for snapshot in snapshots:
        cumulative = ZERO
        for level in snapshot.ask_levels:
            cumulative = _quantize_money(cumulative + level.size)
            if minimum_order_floor <= cumulative <= max_feasible_size:
                sizes.add(cumulative)

    return sorted(sizes)


def _evaluate_size(
    size: Decimal,
    snapshots: list[ExecutableMarketSnapshotInput],
    top_of_book_cost_per_bundle: Decimal,
) -> ExecutionPoint:
    gross_cost_usd = ZERO
    fee_cost_usd = ZERO
    slippage_cost_usd = ZERO

    for snapshot in snapshots:
        fill = _consume_ask_levels(snapshot.ask_levels, size)
        if fill.filled_size != size:
            raise ValueError("candidate size exceeds executable depth")
        gross_cost_usd += fill.gross_cost_usd
        fee_cost_usd += _fee_for_fill(fill.consumed_levels, snapshot.taker_fee_bps)
        slippage_cost_usd += fill.gross_cost_usd - _quantize_money(size * snapshot.ask_levels[0].price)

    gross_cost_usd = _quantize_money(gross_cost_usd)
    fee_cost_usd = _quantize_money(fee_cost_usd)
    slippage_cost_usd = _quantize_money(slippage_cost_usd)
    gross_edge = _quantize_edge(BUNDLE_PAYOUT_USD - (gross_cost_usd / size))
    fee_adjusted_edge = _quantize_edge((size * BUNDLE_PAYOUT_USD - gross_cost_usd - fee_cost_usd) / size)
    return ExecutionPoint(
        size=_quantize_money(size),
        gross_cost_usd=gross_cost_usd,
        gross_edge=gross_edge,
        fee_cost_usd=fee_cost_usd,
        fee_adjusted_edge=fee_adjusted_edge,
        slippage_cost_usd=slippage_cost_usd,
    )


@dataclass(frozen=True, slots=True)
class _ConsumedLevel:
    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class _FillResult:
    filled_size: Decimal
    gross_cost_usd: Decimal
    consumed_levels: tuple[_ConsumedLevel, ...]


def _consume_ask_levels(levels: tuple[OrderBookLevelInput, ...], size: Decimal) -> _FillResult:
    remaining = _quantize_money(size)
    filled = ZERO
    gross_cost = ZERO
    consumed: list[_ConsumedLevel] = []

    for level in levels:
        if remaining <= ZERO:
            break
        if level.size <= ZERO:
            continue
        take_size = min(level.size, remaining)
        if take_size <= ZERO:
            continue
        filled = _quantize_money(filled + take_size)
        gross_cost = _quantize_money(gross_cost + (take_size * level.price))
        consumed.append(_ConsumedLevel(price=level.price, size=_quantize_money(take_size)))
        remaining = _quantize_money(remaining - take_size)

    return _FillResult(
        filled_size=_quantize_money(filled),
        gross_cost_usd=_quantize_money(gross_cost),
        consumed_levels=tuple(consumed),
    )


def _fee_for_fill(consumed_levels: tuple[_ConsumedLevel, ...], taker_fee_bps: Decimal) -> Decimal:
    if taker_fee_bps <= ZERO:
        return ZERO

    fee_rate = taker_fee_bps / Decimal("10000")
    total_fee = ZERO
    for level in consumed_levels:
        fee = level.size * fee_rate * level.price * (Decimal("1") - level.price)
        total_fee += fee.quantize(FEE_PRECISION, rounding=ROUND_HALF_UP)
    return total_fee.quantize(FEE_PRECISION, rounding=ROUND_HALF_UP)


def _parse_yes_ask_levels(order_book_payload: dict[str, Any]) -> list[OrderBookLevelInput]:
    tokens = order_book_payload.get("tokens", [])
    pricing_outcome = str(order_book_payload.get("pricing_outcome") or "").strip().lower()
    if not isinstance(tokens, list):
        return []

    selected_tokens = [
        token
        for token in tokens
        if isinstance(token, dict)
        and (
            not pricing_outcome
            or str(token.get("outcome") or "").strip().lower() == pricing_outcome
        )
    ]
    if not selected_tokens:
        selected_tokens = [token for token in tokens if isinstance(token, dict)]

    levels: list[OrderBookLevelInput] = []
    for token in selected_tokens:
        asks = token.get("asks", [])
        if not isinstance(asks, list):
            continue
        for ask in asks:
            if not isinstance(ask, dict):
                continue
            price = _parse_decimal(ask.get("price"))
            size = _parse_decimal(ask.get("size"))
            if price is None or size is None or price <= ZERO or size <= ZERO:
                continue
            levels.append(
                OrderBookLevelInput(
                    price=_quantize_money(price),
                    size=_quantize_money(size),
                )
            )

    return sorted(levels, key=lambda level: (level.price, level.size))


def _extract_taker_fee_bps(raw_market_payload: dict[str, Any]) -> Decimal:
    for key in ("takerBaseFee", "taker_base_fee", "base_fee"):
        parsed = _parse_decimal(raw_market_payload.get(key))
        if parsed is not None:
            return _quantize_money(parsed)
    fees_enabled = raw_market_payload.get("feesEnabled")
    if fees_enabled is False:
        return ZERO
    return Decimal("-1")


def _extract_min_order_size(raw_market_payload: dict[str, Any]) -> Decimal:
    for key in ("orderMinSize", "order_min_size", "mos"):
        parsed = _parse_decimal(raw_market_payload.get(key))
        if parsed is not None and parsed > ZERO:
            return _quantize_money(parsed)
    return ZERO


def _market_audit_payload(
    snapshots: list[ExecutableMarketSnapshotInput],
    *,
    reference_time: datetime,
) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for snapshot in snapshots:
        snapshot_age_seconds = _snapshot_age_seconds(
            captured_at=snapshot.captured_at,
            reference_time=reference_time,
        )
        payload.append(
            {
                "market_id": snapshot.market_id,
                "snapshot_id": snapshot.snapshot_id,
                "captured_at": snapshot.captured_at.isoformat(),
                "snapshot_timestamp": snapshot.captured_at.isoformat(),
                "snapshot_age_seconds": (
                    format(snapshot_age_seconds, "f") if snapshot_age_seconds is not None else None
                ),
                "taker_fee_bps": format(_quantize_money(max(snapshot.taker_fee_bps, ZERO)), "f"),
                "min_order_size": format(snapshot.min_order_size, "f"),
                "fee_source": (
                    "persisted_market_metadata"
                    if snapshot.taker_fee_bps >= ZERO
                    else "persisted_market_metadata_missing"
                ),
                "top_ask_price": (
                    format(snapshot.ask_levels[0].price, "f") if snapshot.ask_levels else None
                ),
                "total_ask_size": format(
                    _quantize_money(sum((level.size for level in snapshot.ask_levels), ZERO)),
                    "f",
                ),
                "ask_levels": [
                    {
                        "price": format(level.price, "f"),
                        "size": format(level.size, "f"),
                    }
                    for level in snapshot.ask_levels
                ],
            }
        )
    return payload


def _snapshot_age_seconds(*, captured_at: datetime, reference_time: datetime) -> Decimal | None:
    normalized_captured_at = captured_at if captured_at.tzinfo is not None else captured_at.replace(tzinfo=UTC)
    normalized_reference_time = (
        reference_time if reference_time.tzinfo is not None else reference_time.replace(tzinfo=UTC)
    )
    delta_seconds = max((normalized_reference_time - normalized_captured_at).total_seconds(), 0.0)
    return _quantize_money(Decimal(str(delta_seconds)))


def _suggested_notional_bucket(size: Decimal) -> str | None:
    for lower, upper, label in SUGGESTED_BUCKETS:
        if lower <= size < upper:
            return label
    return None


def _build_result(
    *,
    status: str,
    reason_code: str,
    summary: str,
    details: dict[str, Any],
    score: Decimal | None = None,
    top_of_book_edge: Decimal | None = None,
    depth_weighted_edge: Decimal | None = None,
    fee_adjusted_edge: Decimal | None = None,
    min_executable_size: Decimal | None = None,
    suggested_notional_bucket: str | None = None,
    execution_size: Decimal | None = None,
    max_positive_size: Decimal | None = None,
    gross_cost_usd: Decimal | None = None,
    gross_payout_usd: Decimal | None = None,
    fee_cost_usd: Decimal | None = None,
    slippage_cost_usd: Decimal | None = None,
) -> ExecutableEdgeValidationResult:
    merged_details = dict(details)
    merged_details["reason_code"] = reason_code
    return ExecutableEdgeValidationResult(
        validation_type=VALIDATION_TYPE,
        status=status,
        score=score,
        summary=summary,
        details=merged_details,
        validator_version=VALIDATOR_VERSION,
        reason_code=reason_code,
        top_of_book_edge=top_of_book_edge,
        depth_weighted_edge=depth_weighted_edge,
        fee_adjusted_edge=fee_adjusted_edge,
        min_executable_size=min_executable_size,
        suggested_notional_bucket=suggested_notional_bucket,
        execution_size=execution_size,
        max_positive_size=max_positive_size,
        gross_cost_usd=gross_cost_usd,
        gross_payout_usd=gross_payout_usd,
        fee_cost_usd=fee_cost_usd,
        slippage_cost_usd=slippage_cost_usd,
    )


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_PRECISION, rounding=ROUND_HALF_UP)


def _quantize_edge(value: Decimal) -> Decimal:
    return value.quantize(EDGE_PRECISION, rounding=ROUND_HALF_UP)


def _quantize_ratio(value: Decimal) -> Decimal:
    return value.quantize(RATIO_PRECISION, rounding=ROUND_HALF_UP)
