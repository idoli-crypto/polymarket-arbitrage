#!/usr/bin/env bash
set -euo pipefail

: "${WORKER_MARKET_LIMIT:=50}"
: "${WORKER_MARKET_SAMPLE_SIZE:=25}"
: "${WORKER_POLL_ITERATIONS:=1}"

python3 -m apps.worker.poll_polymarket \
  --market-limit "${WORKER_MARKET_LIMIT}" \
  --market-sample-size "${WORKER_MARKET_SAMPLE_SIZE}" \
  --iterations "${WORKER_POLL_ITERATIONS}"
python3 -m apps.worker.detect_neg_risk
python3 -m apps.worker.validate_opportunities
python3 -m apps.worker.simulate_execution
python3 -m apps.worker.calculate_kpi
python3 -m apps.worker.score_recommendations
