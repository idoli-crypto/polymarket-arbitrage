# Render Deployment Runbook

## Scope

This repository deploys a recommendation-only Polymarket arbitrage research platform. It does not execute trades, manage wallets, or automate external actions.

## Deployment Surfaces

- API web service: FastAPI application served by `uvicorn`
- Web dashboard: static Vite build
- Worker: Render cron job that runs one full research-to-recommendation cycle per invocation
- Database: Render Postgres

## Repository Commands

Backend install:

```bash
python3 -m pip install -e .
```

API migrate:

```bash
alembic upgrade head
```

API start:

```bash
uvicorn apps.api.main:app --host 0.0.0.0 --port "${PORT:-8000}"
```

Frontend install:

```bash
cd apps/web && npm ci
```

Frontend build:

```bash
cd apps/web && npm run build
```

Worker cycle:

```bash
./scripts/run_worker_cycle.sh
```

## Required Environment Variables

Shared backend and worker:

- `DATABASE_URL`: Postgres connection string. In Render, wire this from the managed Postgres `connectionString`.
- `APP_ENV`: Use `production` on Render.
- `DATABASE_ECHO`: Keep `false` in production unless debugging.
- `API_PREFIX`: Optional. Leave empty with the current API routing layout.

Web dashboard:

- `VITE_API_BASE_URL`: Public base URL for the API service, for example `https://polymarket-arbitrage-api.onrender.com`.

Worker tuning:

- `WORKER_MARKET_LIMIT`: Poll limit per cycle. Default blueprint value is `50`.
- `WORKER_MARKET_SAMPLE_SIZE`: Snapshot sample size per cycle. Default blueprint value is `25`.
- `WORKER_POLL_ITERATIONS`: Keep `1` for cron-driven execution so each run exits cleanly.

Worker runtime consideration:

- Each worker cycle must complete within the configured cron interval.
- Overlapping runs must be avoided.
- If runtime approaches the interval, first reduce `WORKER_MARKET_LIMIT`, then reduce `WORKER_MARKET_SAMPLE_SIZE`, and finally increase the cron interval.

## GitHub Workflow

GitHub Actions workflow: [.github/workflows/ci.yml](/Users/idoli/polymarket_arbitrage/polymarket-arbitrage/.github/workflows/ci.yml)

It runs:

- backend unit and integration tests with `python -m unittest discover -s tests`
- frontend tests with `npm run test`
- frontend production build with `npm run build`

## Render Setup

Blueprint file: [render.yaml](/Users/idoli/polymarket_arbitrage/polymarket-arbitrage/render.yaml)

Recommended rollout order:

1. Push the repository to GitHub.
2. In Render, create a new Blueprint from the repository root.
3. Provision the Postgres instance from the Blueprint.
4. Create the API service and allow its `preDeployCommand` to run `alembic upgrade head`.
5. Set `VITE_API_BASE_URL` on the static web service to the public API URL.
6. Sync the Blueprint again if needed after the API URL is known.
7. Enable or trigger the worker cron job once the API and database are healthy.

## Database Migration Procedure

Local:

```bash
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/polymarket_arbitrage
alembic upgrade head
```

Render:

- API deploys run migrations automatically via `preDeployCommand: alembic upgrade head`.
- For manual verification, open a Render shell on the API service and run `alembic current`.
- Do not rely on local SQLite or filesystem state for production deployments.

## Health and Smoke Checks

API liveness:

```bash
curl "$API_BASE_URL/health"
```

Expected response:

```json
{"status":"ok"}
```

API readiness:

```bash
curl "$API_BASE_URL/health/ready"
```

Expected response contains `status: ok` and `database: ok`.

Recommendation freshness:

```bash
curl "$API_BASE_URL/recommendations/status"
```

Recommendation queue:

```bash
curl "$API_BASE_URL/recommendations?limit=5&sort=score"
```

System status:

```bash
curl "$API_BASE_URL/system/status"
```

Dashboard:

- Open the static web URL.
- Confirm the dashboard loads without a blank screen.
- Confirm queue and system-status views can reach the API.

Worker freshness:

- Trigger the cron job manually once after deployment.
- Recheck `GET /recommendations/status`.
- Confirm `latest_scoring_run_timestamp` updates.
- Confirm `GET /system/status` shows current snapshot and KPI timestamps.
- Confirm the worker run completes before the next scheduled cron window.

Database connectivity:

- `GET /health/ready` must return success.
- API deploy logs must show successful migration completion before the app starts serving traffic.

## Post-Deploy Smoke Test Checklist

- API responds on `/health`
- API readiness responds on `/health/ready`
- `GET /recommendations/status` returns JSON
- `GET /recommendations` returns a response and pagination headers
- `GET /system/status` returns timestamps or explicit `null` values
- Static dashboard loads and can call the API
- Worker cron job can be triggered manually without crashing
- Recommendation freshness updates after a worker run
- No secrets are committed in the repository or hardcoded in `render.yaml`

## Rollback

Application rollback:

1. Roll back the API service to the previous healthy deploy in the Render dashboard.
2. Roll back the static web service to the previous healthy deploy.
3. Disable the worker cron job temporarily if the issue is caused by a bad worker release.

Database rollback:

- Schema rollback is not automatic.
- If a migration must be reversed, review the relevant Alembic revision before running any downgrade.
- If the deploy introduced data risk, prefer restoring from a Render backup or point-in-time recovery instead of improvising manual SQL changes.

## Manual Render Steps That Remain

- Connect the GitHub repository to Render
- Set `VITE_API_BASE_URL` to the final public API URL
- Optionally add custom domains
- Trigger the first worker run after the API service is healthy
- Review Render logs and metrics after the first full cycle
