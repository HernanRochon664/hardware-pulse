# Hardware Pulse Agent Guide

## Developer Commands
- **Ingestion**: `uv run scripts/run_ingest.py`
- **Entity Resolution**: `uv run scripts/run_resolve.py`
- **Feature Engineering**: `uv run scripts/run_features.py --since YYYY-MM-DD` (optional date filter)
- **Tests**: `pytest`
- **Lint/Format**: `ruff check .` and `ruff format .`
- **Typecheck**: `pyright`

## Pipeline Order (Required)
`run_ingest.py` → `run_resolve.py` → `run_features.py`

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
