from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select

from apps.api.db.models import Market, MarketSnapshot
from apps.api.db.session import SessionLocal


def main() -> None:
    with SessionLocal() as session:
        market = session.scalar(select(Market).where(Market.polymarket_market_id == "demo-market-1"))
        if market is None:
            market = Market(
                polymarket_market_id="demo-market-1",
                question="Will this smoke test insert and read data?",
                slug="demo-market-1",
            )
            session.add(market)
            session.flush()

        snapshot = MarketSnapshot(
            market_id=market.id,
            best_bid=Decimal("0.4700"),
            best_ask=Decimal("0.5200"),
            bid_depth_usd=Decimal("47.0000"),
            ask_depth_usd=Decimal("62.4000"),
            captured_at=datetime.now(timezone.utc),
        )
        session.add(snapshot)
        session.commit()

        stored_market = session.scalar(
            select(Market).where(Market.polymarket_market_id == "demo-market-1")
        )
        stored_snapshot = session.scalar(
            select(MarketSnapshot).where(MarketSnapshot.market_id == market.id)
        )

        print(
            {
                "market_id": stored_market.id if stored_market else None,
                "snapshot_id": stored_snapshot.id if stored_snapshot else None,
                "best_bid": str(stored_snapshot.best_bid) if stored_snapshot else None,
                "bid_depth_usd": str(stored_snapshot.bid_depth_usd) if stored_snapshot else None,
            }
        )


if __name__ == "__main__":
    main()
