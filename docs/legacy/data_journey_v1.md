# Data Journey

## Overview

The Hardware Pulse pipeline transforms raw scraped product listings into structured price intelligence. This document traces the data flow from ingestion to prediction.

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌────────────┐
│   Scrapers   │───▶│  Ingestion   │───▶│  Resolution  │───▶│  Pricing   │
│  (Thot, etc) │    │  (raw_list-  │    │  (entity     │    │  (price_   │
│              │    │   ings)      │    │   match)     │    │   snapshots)│
└─────────────┘    └──────────────┘    └──────────────┘    └────────────┘
                                                                    │
                                                                    ▼
┌─────────────┐    ┌──────────────┐    ┌──────────────┐             │
│  Predictions │◀───│  Training    │◀───│  Features    │◀────────────┘
│  (predictions│    │  (artifact   │    │  (feature_   │
│   table)     │    │   .joblib)   │    │   snapshots) │
└─────────────┘    └──────────────┘    └──────────────┘
```

## Stage Details

### 1. Scraping → `raw_listings`

**Entry point:** `scripts/run_ingest.py` → `src/pipelines/ingest.py`

Each scraper (`src/scrapers/*.py`) visits configured retailer URLs, extracts product data, and yields `RawListing` objects.

**Output table:** `raw_listings`
- `source`, `url`, `item_id`, `timestamp`
- `title`, `price`, `currency`, `seller`
- `condition`, `available_quantity`

**Key rules:**
- Scrapers are configured in `configs/scrapers.yaml`
- Rate limits and page counts are enforced per source
- Duplicates are detected via SHA-256 key (`item_id` or normalized URL)
- Existing rows are updated if price changed
- A scraper returning 0 rows writes an alert to `logs/alerts.log`

### 2. Entity Resolution → `price_snapshots`

**Entry point:** `scripts/run_resolve.py` → `src/pipelines/resolve.py`

Raw listings are matched against the canonical catalog (`configs/catalog.yaml`) using a three-strategy pipeline:

1. **Exact match** — normalized title contains normalized SKU (confidence 1.0)
2. **Regex match** — GPU/CPU/RAM pattern extraction + catalog validation (confidence 0.9)
3. **Fuzzy match** — `rapidfuzz.partial_ratio` fallback (confidence >= 0.8)

**Output table:** `price_snapshots`
- `canonical_product_id`, `source`, `seller`
- `price_usd` (normalized to USD via FX rates)
- `timestamp`, `listing_id`

**Key rules:**
- Only NEW-condition listings persist to `price_snapshots`
- UYU prices are converted to USD using the weekly FX rate
- Unmatched listings (`canonical_product_id = NULL`) are logged but not stored
- Brand (`ASUS`, `MSI`, `Gigabyte`, etc.) and variant (`TUF`, `Ventus`, etc.) are extracted

### 3. Feature Engineering → `feature_snapshots`

**Entry point:** `scripts/run_features.py` → `src/pipelines/features.py`

Aggregates price snapshots into weekly features per canonical SKU:
- Weekly median price and dispersion
- Lag features (1 and 2 weeks)
- 4-week rolling median
- USD/UYU FX rate for the week

**Output table:** `feature_snapshots`
- `week_start`, `canonical_product_id`
- `precio_mediana`, `precio_lag_1`, `precio_lag_2`
- `mediana_movil`, `dispersion_precios`
- `usd_uyu_rate`

### 4. Training → Artifact

**Entry point:** `scripts/run_training.py`

Loads feature snapshots, filters SKUs with >= 12 weeks of history, and trains two models per SKU:
- **ElasticNet** — linear regression with L1/L2 regularization
- **NaivePersistence** — predicts next week = last observed value

The best model (lowest MAPE on 20% holdout) is persisted as a `.joblib` artifact with metadata JSON.

**Output:** `artifacts/model_YYYYMMDD.joblib`

### 5. Inference → `predictions`

**Entry point:** `scripts/run_inference.py`

Loads the latest trained artifact, generates price predictions for the most recent complete week, and writes to the `predictions` table.

**Output table:** `predictions`
- `week_start`, `canonical_product_id`
- `predicted_price_usd`, `lower_bound`, `upper_bound`
- `model_id`, `run_at`

### 6. Dashboard

**Entry point:** `scripts/run_dashboard.py` → `src/dashboard/app.py`

Read-only views powered by `price_snapshots` and `predictions`:
- Market summary with per-SKU pricing vs historical median
- Deal detection (configurable threshold via sidebar slider)
- Per-product price history chart (Plotly)
- Current store-level prices

## Entity-Relationship Summary

```
raw_listings ──1:N──▶ price_snapshots ──1:N──▶ feature_snapshots ──1:N──▶ predictions
                           │
                           └── feeds ──▶ dashboard (queries.py)
```

- `price_snapshots` are the central fact table — all downstream stages derive from them
- `feature_snapshots` are weekly aggregates (one row per SKU per week)
- `predictions` are per-SKU weekly forecasts (one row per SKU per future week)
