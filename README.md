# polymarket-arbitrage
Polymarket arbitrage research and execution system focused on real executable edge, Neg Risk opportunities, and production-grade KPI tracking.

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
