from __future__ import annotations

import argparse
import logging
import time

from apps.worker.integrations.polymarket import PolymarketClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Poll Polymarket markets and order books."
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
        logging.info(
            "Fetched %s markets and %s sampled order book sets",
            len(poll_result.raw_markets),
            len(poll_result.sampled_order_books),
        )
        for market in poll_result.sampled_markets:
            snapshots = poll_result.sampled_order_books.get(market.market_id, [])
            logging.info(
                "market=%s question=%s token_count=%s order_books=%s",
                market.market_id,
                market.question,
                len(market.tokens),
                [
                    {
                        "token_id": snapshot.token_id,
                        "bids": len(snapshot.bids),
                        "asks": len(snapshot.asks),
                        "timestamp": snapshot.timestamp,
                    }
                    for snapshot in snapshots
                ],
            )

        completed_iterations += 1
        if args.iterations > 0 and completed_iterations >= args.iterations:
            break

        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
