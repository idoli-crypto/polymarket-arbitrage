from __future__ import annotations

import argparse
import logging
import time

from sqlalchemy import func, select

from apps.api.db.models import MarketSnapshot
from apps.api.db.session import SessionLocal
from apps.worker.integrations.polymarket import PolymarketClient
from apps.worker.persistence import persist_poll_result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Poll Polymarket order books and persist market snapshots."
    )
    parser.add_argument("--market-limit", type=int, default=5)
    parser.add_argument("--market-sample-size", type=int, default=2)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Set to 0 to run forever.",
    )
    return parser


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = build_parser().parse_args()
    client = PolymarketClient()

    completed_iterations = 0
    while args.iterations == 0 or completed_iterations < args.iterations:
        poll_result = client.poll_markets(
            market_limit=args.market_limit,
            market_sample_size=args.market_sample_size,
        )

        with SessionLocal() as session:
            persisted = persist_poll_result(session, poll_result)
            snapshot_count = session.scalar(select(func.count()).select_from(MarketSnapshot))

        logging.info(
            "Persisted %s market snapshots. total_snapshots=%s",
            len(persisted),
            snapshot_count,
        )
        for snapshot in persisted:
            logging.info(
                "snapshot market=%s local_market_id=%s snapshot_id=%s captured_at=%s best_bid=%s best_ask=%s bid_depth_usd=%s ask_depth_usd=%s",
                snapshot.polymarket_market_id,
                snapshot.market_id,
                snapshot.snapshot_id,
                snapshot.captured_at.isoformat(),
                snapshot.best_bid,
                snapshot.best_ask,
                snapshot.bid_depth_usd,
                snapshot.ask_depth_usd,
            )

        completed_iterations += 1
        if args.iterations > 0 and completed_iterations >= args.iterations:
            break

        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
