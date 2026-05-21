# Hardware Pulse Agent Guide

## Developer Commands
- **Full Pipeline** (recommended): `uv run scripts/run_pipeline.py`
- **Ingestion** (standalone): `uv run scripts/run_ingest.py`
- **Entity Resolution** (standalone): `uv run scripts/run_resolve.py`
- **Feature Engineering** (standalone): `uv run scripts/run_features.py --since YYYY-MM-DD`
- **Tests**: `pytest`
- **Lint/Format**: `ruff check .` and `ruff format .`
- **Typecheck**: `pyright`

## Pipeline Order (Required)
> `run_pipeline.py` runs all three stages sequentially and stops on error.
Individual stages: `run_ingest.py` → `run_resolve.py` → `run_features.py`

## Architecture & Flow
- **Pipeline**: `src/scrapers/` → `src/storage/` → `src/pipelines/` (resolution) → `src/features/` → `src/models/`
- **Unit of Analysis**: Canonical product SKU aggregated weekly
- **Data Store**: SQLite at `data/hardware_pulse.db`
- **Target**: Median market price (USD) to reduce outlier impact

## Configuration
- Scraper config: `configs/scrapers.yaml` (enable/disable sources, rate limits)
- Canonical catalog: `configs/catalog.yaml` (product reference data)
- Env vars: `.env` required for ingestion (MercadoLibre credentials)

## Testing & Verification
- Use `pytest`
- Playwright for scraping tests via `pytest-playwright`
- Recommended verification order: `ruff check .` → `pyright` → `pytest`
