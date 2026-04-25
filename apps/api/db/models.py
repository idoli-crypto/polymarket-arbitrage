from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from apps.api.db.base import Base
from apps.api.services.opportunity_classification import DetectionFamily


class Market(Base):
    __tablename__ = "markets"

    id: Mapped[int] = mapped_column(primary_key=True)
    polymarket_market_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    question: Mapped[str] = mapped_column(Text)
    slug: Mapped[str | None] = mapped_column(String(255), unique=True, index=True)
    condition_id: Mapped[str | None] = mapped_column(String(255), index=True)
    event_id: Mapped[str | None] = mapped_column(String(255), index=True)
    event_slug: Mapped[str | None] = mapped_column(String(255))
    raw_market_json: Mapped[dict | list | None] = mapped_column(JSON)
    neg_risk: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        server_default=text("false"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(String(50), default="active", server_default=text("'active'"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    snapshots: Mapped[list["MarketSnapshot"]] = relationship(
        back_populates="market",
        cascade="all, delete-orphan",
    )


class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"
    __table_args__ = (
        UniqueConstraint("market_id", "captured_at", name="uq_market_snapshots_market_captured_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    market_id: Mapped[int] = mapped_column(ForeignKey("markets.id", ondelete="CASCADE"), index=True)
    best_bid: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    best_ask: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    bid_depth_usd: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    ask_depth_usd: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    order_book_json: Mapped[dict | list | None] = mapped_column(JSON)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    market: Mapped[Market] = relationship(back_populates="snapshots")


class DetectedOpportunity(Base):
    __tablename__ = "detected_opportunities"
    __table_args__ = (
        UniqueConstraint(
            "event_group_key",
            "opportunity_type",
            "detector_version",
            "detection_window_start",
            name="uq_detected_opportunities_event_type_version_window",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True,
    )
    detection_window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event_group_key: Mapped[str] = mapped_column(String(255), index=True)
    involved_market_ids: Mapped[list[int]] = mapped_column(JSON, nullable=False)
    opportunity_type: Mapped[str] = mapped_column(String(100), nullable=False)
    outcome_count: Mapped[int] = mapped_column(nullable=False)
    gross_price_sum: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    gross_gap: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    family: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        default=DetectionFamily.NEG_RISK_CONVERSION.value,
        server_default=text(f"'{DetectionFamily.NEG_RISK_CONVERSION.value}'"),
    )
    relation_type: Mapped[str | None] = mapped_column(String(100))
    relation_direction: Mapped[str | None] = mapped_column(String(100))
    involved_market_ids_json: Mapped[list[int] | None] = mapped_column(JSON)
    question_texts_json: Mapped[list[str] | None] = mapped_column(JSON)
    normalized_entities_json: Mapped[dict | list | None] = mapped_column(JSON)
    normalized_dates_json: Mapped[dict | list | None] = mapped_column(JSON)
    normalized_thresholds_json: Mapped[dict | list | None] = mapped_column(JSON)
    resolution_sources_json: Mapped[dict | list | None] = mapped_column(JSON)
    end_dates_json: Mapped[dict | list | None] = mapped_column(JSON)
    clarification_flags_json: Mapped[dict | list | None] = mapped_column(JSON)
    dispute_flags_json: Mapped[dict | list | None] = mapped_column(JSON)
    s_logic: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    s_sem: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    s_res: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    confidence_tier: Mapped[str | None] = mapped_column(String(20))
    top_of_book_edge: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    depth_weighted_edge: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    fee_adjusted_edge: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    min_executable_size: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    suggested_notional_bucket: Mapped[str | None] = mapped_column(String(50))
    persistence_seconds_estimate: Mapped[int | None] = mapped_column()
    capital_lock_estimate_hours: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    detector_version: Mapped[str] = mapped_column(String(50), nullable=False)
    validation_version: Mapped[str | None] = mapped_column(String(50))
    simulation_version: Mapped[str | None] = mapped_column(String(50))
    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="detected",
        server_default=text("'detected'"),
    )
    validation_status: Mapped[str | None] = mapped_column(String(50))
    validation_reason: Mapped[str | None] = mapped_column(String(50))
    validated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    risk_flags_json: Mapped[dict | list | None] = mapped_column(JSON)
    recommendation_eligibility: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    recommendation_block_reason: Mapped[str | None] = mapped_column(String(255))
    raw_context: Mapped[dict | None] = mapped_column(JSON)

    simulations: Mapped[list["ExecutionSimulation"]] = relationship(
        back_populates="opportunity",
        cascade="all, delete-orphan",
    )
    validation_results: Mapped[list["ValidationResult"]] = relationship(
        back_populates="opportunity",
        cascade="all, delete-orphan",
    )
    simulation_results: Mapped[list["SimulationResult"]] = relationship(
        back_populates="opportunity",
        cascade="all, delete-orphan",
    )
    recommendation_scores: Mapped[list["RecommendationScore"]] = relationship(
        back_populates="opportunity",
        cascade="all, delete-orphan",
    )
    kpi_snapshots: Mapped[list["OpportunityKpiSnapshot"]] = relationship(
        back_populates="opportunity",
        cascade="all, delete-orphan",
    )


class ExecutionSimulation(Base):
    __tablename__ = "execution_simulations"

    id: Mapped[int] = mapped_column(primary_key=True)
    opportunity_id: Mapped[int] = mapped_column(
        ForeignKey("detected_opportunities.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    simulated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    simulation_status: Mapped[str] = mapped_column(String(50), nullable=False)
    intended_size_usd: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    executable_size_usd: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    gross_cost_usd: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    gross_payout_usd: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    estimated_fees_usd: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    estimated_slippage_usd: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    estimated_net_edge_usd: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    fill_completion_ratio: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    simulation_reason: Mapped[str | None] = mapped_column(String(50))
    raw_context: Mapped[dict | None] = mapped_column(JSON)

    opportunity: Mapped["DetectedOpportunity"] = relationship(back_populates="simulations")


class ValidationResult(Base):
    __tablename__ = "validation_results"

    id: Mapped[int] = mapped_column(primary_key=True)
    opportunity_id: Mapped[int] = mapped_column(
        ForeignKey("detected_opportunities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    validation_type: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    score: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    summary: Mapped[str | None] = mapped_column(Text)
    details_json: Mapped[dict | list | None] = mapped_column(JSON)
    validator_version: Mapped[str] = mapped_column(String(50), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True,
    )

    opportunity: Mapped["DetectedOpportunity"] = relationship(back_populates="validation_results")


class SimulationResult(Base):
    __tablename__ = "simulation_results"

    id: Mapped[int] = mapped_column(primary_key=True)
    opportunity_id: Mapped[int] = mapped_column(
        ForeignKey("detected_opportunities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    simulation_mode: Mapped[str] = mapped_column(String(100), nullable=False)
    executable_edge: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    fee_cost: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    slippage_cost: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    estimated_fill_quality: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    fill_completion_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    execution_feasible: Mapped[bool | None] = mapped_column(Boolean)
    min_executable_size: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    suggested_notional_bucket: Mapped[str | None] = mapped_column(String(50))
    persistence_seconds_estimate: Mapped[int | None] = mapped_column()
    capital_lock_estimate_hours: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    execution_risk_flag: Mapped[str | None] = mapped_column(String(50))
    simulation_version: Mapped[str] = mapped_column(String(50), nullable=False)
    details_json: Mapped[dict | list | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True,
    )

    opportunity: Mapped["DetectedOpportunity"] = relationship(back_populates="simulation_results")


class RecommendationScore(Base):
    __tablename__ = "recommendation_scores"

    id: Mapped[int] = mapped_column(primary_key=True)
    opportunity_id: Mapped[int] = mapped_column(
        ForeignKey("detected_opportunities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    score: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    tier: Mapped[str | None] = mapped_column(String(20))
    reason_summary: Mapped[str | None] = mapped_column(Text)
    warning_summary: Mapped[str | None] = mapped_column(Text)
    manual_review_required: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    scoring_version: Mapped[str] = mapped_column(String(50), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True,
    )

    opportunity: Mapped["DetectedOpportunity"] = relationship(back_populates="recommendation_scores")


class RecommendationScoringRun(Base):
    __tablename__ = "recommendation_scoring_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    worker_status: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    opportunities_scored: Mapped[int] = mapped_column(nullable=False, server_default=text("0"))
    high_conviction_count: Mapped[int] = mapped_column(nullable=False, server_default=text("0"))
    review_count: Mapped[int] = mapped_column(nullable=False, server_default=text("0"))
    blocked_count: Mapped[int] = mapped_column(nullable=False, server_default=text("0"))
    scoring_version: Mapped[str] = mapped_column(String(50), nullable=False)
    run_reason: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True,
    )


class KpiSnapshot(Base):
    __tablename__ = "kpi_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True,
    )
    total_opportunities: Mapped[int] = mapped_column(nullable=False)
    valid_opportunities: Mapped[int] = mapped_column(nullable=False)
    executable_opportunities: Mapped[int] = mapped_column(nullable=False)
    partial_opportunities: Mapped[int] = mapped_column(nullable=False)
    rejected_opportunities: Mapped[int] = mapped_column(nullable=False)
    avg_real_edge: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    avg_fill_ratio: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    false_positive_rate: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    total_intended_capital: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    total_executable_capital: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    raw_context: Mapped[dict] = mapped_column(JSON, nullable=False)


class KpiRunSummary(Base):
    __tablename__ = "kpi_run_summary"

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True,
    )
    run_started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    run_completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    kpi_version: Mapped[str] = mapped_column(String(50), nullable=False)
    total_opportunities: Mapped[int] = mapped_column(nullable=False)
    valid_after_rule: Mapped[int] = mapped_column(nullable=False)
    valid_after_semantic: Mapped[int] = mapped_column(nullable=False)
    valid_after_resolution: Mapped[int] = mapped_column(nullable=False)
    valid_after_executable: Mapped[int] = mapped_column(nullable=False)
    valid_after_simulation: Mapped[int] = mapped_column(nullable=False)
    avg_executable_edge: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    avg_fill_ratio: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    avg_capital_lock: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    false_positive_rate: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    family_distribution: Mapped[dict] = mapped_column(JSON, nullable=False)
    detector_versions_json: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    validation_versions_json: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    simulation_versions_json: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    raw_context: Mapped[dict] = mapped_column(JSON, nullable=False)

    opportunity_snapshots: Mapped[list["OpportunityKpiSnapshot"]] = relationship(
        back_populates="run_summary",
        cascade="all, delete-orphan",
    )


class OpportunityKpiSnapshot(Base):
    __tablename__ = "opportunity_kpi_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True)
    run_summary_id: Mapped[int] = mapped_column(
        ForeignKey("kpi_run_summary.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    opportunity_id: Mapped[int] = mapped_column(
        ForeignKey("detected_opportunities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    lineage_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    kpi_version: Mapped[str] = mapped_column(String(50), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True,
    )
    snapshot_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    family: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    validation_stage_reached: Mapped[str] = mapped_column(String(50), nullable=False)
    final_status: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    rejection_stage: Mapped[str | None] = mapped_column(String(50), index=True)
    rejection_reason: Mapped[str | None] = mapped_column(String(255))
    detected: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
    )
    rule_pass: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    semantic_pass: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    resolution_pass: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    executable_pass: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    simulation_pass: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    s_logic: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    s_sem: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    s_res: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    top_of_book_edge: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    depth_weighted_edge: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    fee_adjusted_edge: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    fill_completion_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    execution_feasible: Mapped[bool | None] = mapped_column(Boolean)
    capital_lock_estimate_hours: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    detector_version: Mapped[str] = mapped_column(String(50), nullable=False)
    validation_version: Mapped[str | None] = mapped_column(String(50))
    simulation_version: Mapped[str | None] = mapped_column(String(50))
    first_seen_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    persistence_duration_seconds: Mapped[int] = mapped_column(nullable=False)
    decay_status: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    raw_context: Mapped[dict] = mapped_column(JSON, nullable=False)

    opportunity: Mapped[DetectedOpportunity] = relationship(back_populates="kpi_snapshots")
    run_summary: Mapped[KpiRunSummary] = relationship(back_populates="opportunity_snapshots")
