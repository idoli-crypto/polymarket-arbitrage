from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, JSON, Numeric, String, Text, UniqueConstraint, func, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from apps.api.db.base import Base


class Market(Base):
    __tablename__ = "markets"

    id: Mapped[int] = mapped_column(primary_key=True)
    polymarket_market_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    question: Mapped[str] = mapped_column(Text)
    slug: Mapped[str | None] = mapped_column(String(255), unique=True, index=True)
    condition_id: Mapped[str | None] = mapped_column(String(255), index=True)
    event_id: Mapped[str | None] = mapped_column(String(255), index=True)
    event_slug: Mapped[str | None] = mapped_column(String(255))
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
    detector_version: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="detected",
        server_default=text("'detected'"),
    )
    raw_context: Mapped[dict | None] = mapped_column(JSON)
