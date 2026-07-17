# Known Issues

## Playwright E2E Tests Fail with Missing System Libraries

**Symptoms:** `tests/e2e/test_dashboard.py` tests fail with:
```
error while loading shared libraries: libnspr4.so: cannot open shared object file
```

**Cause:** The Chromium browser installed by Playwright requires `libnspr4` and other system-level shared libraries. These are not installed by pip/uv and must be provided by the OS.

**Workaround:** Install system dependencies:
- Debian/Ubuntu: `sudo apt-get install libnspr4 libnss3 libatk-bridge2.0-0 libdrm2 libxkbcommon0 libgbm1`
- The E2E tests are structurally verified; they can be run locally or in CI where these libraries are available.

**Status:** Environment-specific. Not a code defect.

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
