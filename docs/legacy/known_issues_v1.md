# Known Issues

## Playwright E2E Tests (Resolved)

**History:** `tests/e2e/test_dashboard.py` initially failed due to missing system libraries (`libnspr4`, `libnss3`, etc.) and incorrect selectors for Streamlit 1.57+ (slider uses `stSlider` div, not `input[type="range"]`; DataFrame uses `stDataFrame`, not `stTable`).

**Resolution:** System dependencies installed and selectors updated. All 11 E2E tests pass.

**Relevant commits:** `7523b9b`

## Hardcoded ±10% Confidence Intervals

**Location:** `scripts/run_inference.py:112-113`

**Issue:** Prediction bounds are hardcoded as `pred_val * 0.9` and `pred_val * 1.1` instead of being derived from model uncertainty or historical forecast error.

**Planned fix:** Replace with a dynamic interval based on historical MAPE or residual standard deviation from the last training run.

**Status:** Accepted for MVP. Tracked for Phase 4.

## Schema Migrations Not Implemented

**Issue:** The database schema is applied via `CREATE TABLE IF NOT EXISTS` statements. There is no migration system for evolving the schema over time (adding columns, renaming fields, backfilling data).

**Planned fix:** Implement a `schema_version` table and sequential migration files in `src/storage/migrations/`.

**Status:** Deferred. Schema changes require manual `DROP TABLE` or `ALTER TABLE` for now.

## Structured Logging Not Implemented

**Issue:** The pipeline uses Python's stdlib `logging` with plain-text formatting. There is no structured logging (JSON, log correlation IDs) for centralized log aggregation.

**Planned fix:** Replace `logging.basicConfig` with a structured logger (e.g. `structlog`) that emits JSON-formatted log entries with `run_id` correlation across pipeline stages.

**Status:** Deferred. Not critical for single-machine execution.

## Workaround: `pythonpath = ["."]` in pyproject.toml

**Issue:** Added `pythonpath = ["."]` to `[tool.pytest.ini_options]` in pyproject.toml to allow notebooks to import `src.*` modules without explicit `sys.path` manipulation. This is a pytest workaround, not a production configuration.

**Status:** Acceptable for development. CI should use explicit `sys.path` or `PYTHONPATH` instead.
