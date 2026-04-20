from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, ForeignKey, Numeric, String, Text, UniqueConstraint, func, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from apps.api.db.base import Base


class Market(Base):
    __tablename__ = "markets"

    id: Mapped[int] = mapped_column(primary_key=True)
    polymarket_market_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    question: Mapped[str] = mapped_column(Text)
    slug: Mapped[str | None] = mapped_column(String(255), unique=True, index=True)
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
    last_traded_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    bid_size: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    ask_size: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    market: Mapped[Market] = relationship(back_populates="snapshots")
