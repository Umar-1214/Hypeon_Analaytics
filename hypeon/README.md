# HypeOn Analytics

Product intelligence for multi-channel ad attribution and MMM: Meta, Google, Bing, Pinterest ads; Shopify and WooCommerce orders. Three attribution approaches: **Click-ID**, **MTA** (fractional/Markov), and **MMM** (custom Ridge; Meridian/Robyn documented for future). Data is ingested from CSVs in `data/raw/` (dummy data or future pipeline output). API + web dashboard in one deploy.

## Setup

- **Python 3.11+**
- **PostgreSQL** (local or via Docker)
- Create a virtualenv and install: `pip install -e ".[dev]"`

## Database

**Postgres with Docker (recommended)** — uses host port **5433** to avoid conflict with a local Postgres on 5432:

```bash
docker compose -f infra/compose/docker-compose.yml up -d postgres
```

Then set `DATABASE_URL=postgresql://postgres:postgres@localhost:5433/hypeon` and run migrations (see below).

**Or** use a local Postgres and set `DATABASE_URL` (default: `postgresql://postgres:postgres@localhost:5432/hypeon`).

**Run migrations:**

```bash
# From hypeon/ (Windows PowerShell example)
$env:DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/hypeon"   # or 5432 if local
$env:PYTHONPATH = "."
python -m alembic -c infra/migrations/alembic.ini upgrade head
```

On macOS/Linux: `./scripts/setup_db.sh` (or set `DATABASE_URL` and run the `alembic` command above).

## Sample data (dummy pipeline)

To generate 90 days of realistic sample data (Meta, Google, Bing, Pinterest ads; Shopify and WooCommerce orders; ad clicks for click-ID attribution):

```bash
python scripts/generate_sample_data.py
```

Writes to `data/raw/`: `meta_ads.csv`, `google_ads.csv`, `bing_ads.csv`, `pinterest_ads.csv`, `shopify_orders.csv`, `woocommerce_orders.csv`, `ad_clicks.csv` (date range 2025-01-01 to 2025-03-31). Then run the pipeline (or use the Dashboard **Run pipeline** button). See [design/ingest.md](design/ingest.md) for the CSV contract when you plug in the real pipeline.

## Run pipeline

From repo root (e.g. `hypeon/`):

```bash
./scripts/run_product_engine.sh --seed 42
```

This will:

1. Upsert CSVs from `data/raw/` into raw tables
2. Run attribution → MMM → metrics → rules
3. Write results to `attribution_events`, `mmm_results`, `unified_daily_metrics`, `decision_store`

## Run API

**With Docker (Postgres + API + Web):**

```bash
docker compose -f infra/compose/docker-compose.yml up --build
```

Serves the API at http://localhost:8000 and the web app at http://localhost:8000 (static frontend built into the image). API docs: http://localhost:8000/docs.

**Local (uvicorn + separate Vite dev server):**

```bash
# If using Docker Postgres (port 5433):
export DATABASE_URL=postgresql://postgres:postgres@localhost:5433/hypeon
# Or local Postgres on 5432:
# export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/hypeon
export PYTHONPATH=.   # or path to hypeon repo root
uvicorn apps.api.src.app:app --reload --host 0.0.0.0 --port 8000
```

## Sample API usage

- **Liveness:** `curl http://localhost:8000/health`
- **Unified metrics:** `curl "http://localhost:8000/metrics/unified?start_date=2025-01-01&end_date=2025-01-31"`
- **Decisions:** `curl http://localhost:8000/decisions`
- **Trigger pipeline:** `curl -X POST "http://localhost:8000/run?seed=42"`
- **MMM status:** `curl http://localhost:8000/model/mmm/status`
- **MMM results:** `curl http://localhost:8000/model/mmm/results`
- **Budget optimizer:** `curl "http://localhost:8000/optimizer/budget?total_budget=1000"`
- **Simulate spend changes:** `curl -X POST http://localhost:8000/simulate -H "Content-Type: application/json" -d "{\"meta_spend_change\": 0.2, \"google_spend_change\": -0.1}"`
- **Attribution vs MMM report:** `curl http://localhost:8000/report/attribution-mmm-comparison`

## Tests

```bash
pytest packages apps tests -v
```

## Production

- **One-command run:** `docker compose -f infra/compose/docker-compose.yml up --build`. Ensure `data/raw/` is mounted (e.g. volume in compose) if you use file-based ingest.
- **Environment:** Copy `.env.example` to `.env` and set at least `DATABASE_URL`. Optional: `API_KEY` (enforces X-API-Key or Authorization: Bearer), `CORS_ORIGINS`, `LOG_LEVEL`, `PIPELINE_RUN_INTERVAL_MINUTES` (scheduled runs), `DATA_RAW_DIR`. Do not commit `.env`.
- **Migrations:** Run before first start: `python -m alembic -c infra/migrations/alembic.ini upgrade head` (with `DATABASE_URL` and `PYTHONPATH=.` set).
- **When the real pipeline is ready:** Point `DATA_RAW_DIR` at the directory where your pipeline writes the same CSV filenames/schemas (see [design/ingest.md](design/ingest.md)), or replace the CSV loaders in `packages/shared/src/ingest.py` with your connector (S3, GCS, API). Attribution and MMM consume the same raw tables.

## Architecture

See [design/arch.md](design/arch.md) for a one-page design and data flow. [design/mmm.md](design/mmm.md) for MMM and future Meridian/Robyn.
