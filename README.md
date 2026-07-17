# Hardware Pulse

A data pipeline that tracks and analyzes price dynamics of PC components in the Uruguayan electronics market.
The system collects product listings from local retailers, resolves product identities across stores, builds a historical price dataset, trains forecasting models, and generates price predictions to detect pricing inefficiencies.

---

## Motivation

PC hardware prices in Uruguay show high dispersion due to:

- small market size
- fragmented retail channels
- import costs
- exchange rate fluctuations

Consumers and resellers often struggle to know whether a listing represents a good deal.
This project builds a **price intelligence system** that detects how global events (product launches, FX changes, promotions) propagate into local prices.

---

## Scope (MVP)

The project focuses on four component categories:

- GPUs
- CPUs
- SSDs
- RAM

These were chosen because they:
- have standardized product SKUs
- appear across multiple retailers
- exhibit meaningful price volatility
- have global price signals

---

## Data Sources

The system collects listings from:

### Local retailers
- **Thot Computación** — static WooCommerce site, paginated HTML
- **Banifox** — internal JSON endpoint
- **PCCompu** — server-rendered HTML with `?pagina=N` pagination

### Excluded
- **MercadoLibre** — API returns 403 (PolicyAgent) from Uruguay; Playwright also blocked
- **PC Store Uruguay** — Cloudflare + dynamic CSRF tokens

Each listing contains: title, price, currency, seller, timestamp, product URL.

---

## Architecture

```
Scrapers → Ingestion → Resolution → Feature Engineering → Training → Inference → Dashboard
```

### Pipeline Stages

1. **Ingestion** (`scripts/run_ingest.py`) — Runs configured scrapers, upserts raw listings into SQLite. Alerts on empty scraper results.

2. **Entity Resolution** (`scripts/run_resolve.py`) — Matches raw titles against a canonical catalog (`configs/catalog.yaml`) via three strategies in priority order:
   - **Exact match** — normalized title contains normalized SKU (confidence 1.0)
   - **Regex match** — GPU/CPU/RAM pattern extraction + catalog validation (confidence 0.9)
   - **Fuzzy match** — `rapidfuzz.partial_ratio` fallback (confidence >= 0.8)

   UYU prices are converted to USD using weekly FX rates. Only NEW-condition listings are persisted.

3. **Feature Engineering** (`scripts/run_features.py`) — Aggregates price snapshots into weekly features per SKU: median price, lag features, rolling median, dispersion, FX rate.

4. **Training** (`scripts/run_train.py`) — Trains ElasticNet and NaivePersistence models per SKU (minimum 12 weeks history). Selects winner by MAPE, persists as `.joblib` artifact.

5. **Inference** (`scripts/run_inference.py`) — Loads latest artifact, generates predictions, writes to `predictions` table with confidence bounds.

6. **Dashboard** (`scripts/run_dashboard.py`) — Streamlit UI for market summary, deal detection, and per-product price history charts.

### Master Pipeline

The full pipeline can be run with:
```bash
uv run scripts/run_pipeline.py
```

Optional flags: `--since YYYY-MM-DD`, `--with-train`, `--with-inference`

---

## Product Matching

Product names vary significantly between stores. Example:
- `RTX 4070 Super ASUS Dual`
- `ASUS Dual RTX4070S 12GB OC`
- `Placa de Video ASUS 4070 SUPER 12G`

The matching pipeline:
1. Normalize text (unicode NFKD, separator normalization, noise word removal)
2. Extract brand (ASUS, MSI, Gigabyte, etc.) and variant (TUF, Ventus, etc.)
3. Exact match → regex match → fuzzy matching against catalog

---

## Tech Stack

| Layer | Technology |
|---|---|
| Scraping | requests, beautifulsoup4, Playwright (tests) |
| Data | pandas, SQLite |
| Entity Resolution | rapidfuzz |
| ML | scikit-learn (ElasticNet), joblib |
| Visualization | Streamlit, Plotly |
| Quality | pytest, pyright, ruff |

---

## Project Structure

```
hardware-pulse/
├── configs/           # Scraper and catalog YAML configs
├── data/              # SQLite database
├── docs/              # Architecture decisions and data journey
├── scripts/           # Pipeline entry points
├── src/
│   ├── scrapers/      # Source-specific scrapers
│   ├── storage/       # SQLite schema and repository
│   ├── entities/      # Catalog, normalizer, matcher, resolver
│   ├── pipelines/     # Ingestion, resolution, features, FX
│   ├── models/        # ElasticNet, NaivePersistence, evaluation
│   └── dashboard/     # Streamlit app, queries, signals
├── tests/             # Unit, integration, and E2E tests
└── artifacts/         # Trained model artifacts
```

---

## Tests

```bash
pytest                       # All tests
pytest tests/e2e/           # Dashboard E2E (requires Playwright)
pytest --cov=src            # With coverage
ruff check .                # Lint
ruff format .               # Format
pyright                     # Type check
```

---

## License

MIT
