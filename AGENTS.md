# Hardware Pulse Agent Guide

## Developer Commands
- **Full pipeline**: `uv run python -m src.cli run` (scrape → resolve → aggregate → signals)
- **Without network**: `uv run python -m src.cli run --skip-scrape`
- **Single stages**: `scrape [--dry-run]`, `resolve [--full]`, `aggregate`, `train`, `signals`, `status`
- **Dashboard**: `uv run streamlit run streamlit_app.py` (`HP_DB_PATH` points it at a local DB)
- **Published data**: `uv run python -m src.cli pull-data`
- **Lint/format**: `uv run ruff check .` and `uv run ruff format .`
- **Typecheck**: `uv run pyright`
- **Tests**: `uv run pytest` (offline); `uv run pytest -m e2e tests/e2e`; `uv run pytest -m live`
- Recommended verification order: ruff → pyright → pytest

## Architecture & Flow
- `src/scrapers/` (3 platform adapters, 10 stores) → `src/storage/` (SQLite + migrations)
  → `src/entities/` (strict spec parsers) → `src/pipelines/` → `src/ml/` → `src/dashboard/`
- **Unit of analysis**: exact product (fingerprint), grouped into families (chip/model)
- **History**: `price_periods`, change-based; derived tables are rebuilt on every run
- **Current price**: min USD over active, new, in-stock, fresh, issue-free offers
- **Data store**: `data/hardware_pulse.db` locally; published as the `data-latest` Release asset

## Configuration
- Stores: `configs/stores.yaml` (platform, categories, delays)
- Taxonomy: `configs/taxonomy.yaml` (brands, lines, modifiers, condition/accessory words)
- Paths and time zone: `src/settings.py` (`HP_DB_PATH`, `HP_DATA_URL`)

## Rules
- Never merge products by guessing; extend the gold set when changing resolution.
- Schema changes go in a new file under `src/storage/migrations/`.
- Scheduled scraping runs in GitHub Actions; ask before pushing, releasing or running workflows.
