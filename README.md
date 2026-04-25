# polymarket-arbitrage
Polymarket arbitrage research and recommendation platform focused on real executable edge, Neg Risk opportunities, and production-grade KPI tracking.

The system is recommendation-only. It does not execute trades, submit wallet actions, or automate market participation.

## Order Book Snapshot Ingestion

Issue `#4` adds a minimal worker flow that:

- fetches Polymarket markets and order books
- creates missing `markets` rows
- persists aggregated order book snapshots into `market_snapshots`

Local run:

```bash
export DATABASE_URL=sqlite:///./polymarket.db
python3 -m pip install -e .
alembic upgrade head
python3 -m apps.worker.poll_polymarket --market-limit 5 --market-sample-size 2 --iterations 1
```

Set `--iterations 0` to keep polling continuously.

## Neg Risk Candidate Detection

Issue `#5` adds a detection-only Neg Risk scan that:

- reads stored `markets` plus each market's latest `market_snapshots` row
- groups markets conservatively by stored Polymarket event metadata
- computes only gross long-side bundle pricing
- persists candidate rows into `detected_opportunities`

Local run:

```bash
export DATABASE_URL=sqlite:///./polymarket.db
python3 -m pip install -e .
alembic upgrade head
python3 -m apps.worker.poll_polymarket --market-limit 50 --market-sample-size 25 --iterations 1
python3 -m apps.worker.detect_neg_risk
```

## Semantic Validation

Issue `#6` adds a validation-only Neg Risk pass that:

- reads pending `detected_opportunities`
- loads referenced `markets` and each market's latest `market_snapshots` row
- assigns `validation_status`, `validation_reason`, and `validated_at`
- preserves detector output while marking semantic mismatches and missing snapshots

Local run:

```bash
export DATABASE_URL=sqlite:///./polymarket.db
python3 -m pip install -e .
alembic upgrade head
python3 -m apps.worker.poll_polymarket --market-limit 50 --market-sample-size 25 --iterations 1
python3 -m apps.worker.detect_neg_risk
python3 -m apps.worker.validate_opportunities
```

## Execution Simulation

Issue `#7` adds the first execution-simulation layer that:

- reads validated `detected_opportunities` with no prior simulation row
- uses the latest stored `market_snapshots` per involved market
- simulates conservative long-side bundle execution from stored best ask plus ask depth only
- persists results into `execution_simulations`

Local run:

```bash
export DATABASE_URL=sqlite:///./polymarket.db
python3 -m pip install -e .
alembic upgrade head
python3 -m apps.worker.poll_polymarket --market-limit 50 --market-sample-size 25 --iterations 1
python3 -m apps.worker.detect_neg_risk
python3 -m apps.worker.validate_opportunities
python3 -m apps.worker.simulate_execution
```

## KPI Measurement

Issue `#8` adds the measure-stage KPI layer that:

- reads persisted `execution_simulations` and linked `detected_opportunities`
- computes opportunity-level executable-edge metrics from validated simulations only
- persists aggregate point-in-time snapshots into `kpi_snapshots`
- keeps validation-stage false positives separate from execution-stage rejections

Local run:

```bash
export DATABASE_URL=sqlite:///./polymarket.db
python3 -m pip install -e .
alembic upgrade head
python3 -m apps.worker.poll_polymarket --market-limit 50 --market-sample-size 25 --iterations 1
python3 -m apps.worker.detect_neg_risk
python3 -m apps.worker.validate_opportunities
python3 -m apps.worker.simulate_execution
python3 -m apps.worker.calculate_kpi
```

## Recommendation Scoring

PR 10 adds a recommendation-only scoring layer that:

- reads persisted `detected_opportunities`, `validation_results`, `simulation_results`, and `opportunity_kpi_snapshots`
- writes ranked recommendation outputs into `recommendation_scores`
- writes worker freshness and last-run metadata into `recommendation_scoring_runs`
- never executes trades and does not build UI

Worker contract:

- run `python3 -m apps.worker.score_recommendations`
- run it after validation and KPI persistence so recommendation freshness reflects the latest evidence
- dashboard or API consumers should check `GET /recommendations/status` to determine whether the queue is fresh, stale, missing, or failed before trusting the recommendation list

Local run:

```bash
export DATABASE_URL=sqlite:///./polymarket.db
python3 -m pip install -e .
alembic upgrade head
python3 -m apps.worker.poll_polymarket --market-limit 50 --market-sample-size 25 --iterations 1
python3 -m apps.worker.detect_neg_risk
python3 -m apps.worker.validate_opportunities
python3 -m apps.worker.calculate_kpi
python3 -m apps.worker.score_recommendations
```

## Deployment Readiness

The repository is prepared for GitHub CI and Render rollout:

- `render.yaml` defines the API service, static web dashboard, scheduled worker, and Render Postgres database
- `scripts/run_worker_cycle.sh` runs the existing ingestion and recommendation pipeline once per worker cycle without adding execution logic
- `GET /health` provides a liveness check and `GET /health/ready` verifies database readiness for Render health checks
- `.github/workflows/ci.yml` runs backend tests plus frontend test and build validation on push and pull request

Deployment steps, required environment variables, migration procedure, smoke checks, and rollback guidance are documented in [docs/deployment-render.md](/Users/idoli/polymarket_arbitrage/polymarket-arbitrage/docs/deployment-render.md).
