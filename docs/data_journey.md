# Data Journey (v2)

How one store listing becomes a price on the dashboard. v1's journey is
archived in [`legacy/data_journey_v1.md`](legacy/data_journey_v1.md).

1. **Scrape** (`src/pipelines/scrape.py`, adapters in `src/scrapers/`). Each
   enabled store × category opens a `scrape_runs` row. The adapter yields
   `ScrapedListing`s (title, price, regular price, currency, stock, SKU,
   brand, image). Prices are parsed with explicit separators and converted to
   USD with the day's rate (`fx_rates`, `src/pipelines/fx.py`).
2. **Record** (`src/storage/repository.py`). The listing is upserted by
   canonical URL and its price period is extended or a new one opens. The run is
   classified `ok`, `empty`, `anomaly` or `error`, and only an `ok` run
   deactivates the listings it did not see.
3. **Resolve** (`src/pipelines/resolve.py`, `src/entities/`). New, retitled or
   stale-resolver listings are processed: condition (open box, used…),
   accessory filter, category spec parser, MPN and then a family and a product
   fingerprint. Unparsed titles are kept with `match_status='unparsed'`.
4. **Aggregate** (`src/pipelines/aggregate.py`). This step:
   - flags implausible prices in `quality_issues`;
   - expands periods into the daily lowest valid price per product
     (`product_daily`) and per family (`family_daily`);
   - computes `product_stats`: best current offer, 180-day min and max, 7- and 30-day
     changes, last change and historic-low flag.
5. **Signal** (`src/ml/`). Retrains weekly. If the model passes the gate,
   `signals` gets a probability and a recommendation for each product with enough
   history.
6. **Publish** (`.github/workflows/scrape.yml`). The DB and `status.json` go to the
   `data-latest` Release.
7. **Show** (`streamlit_app.py`, `src/dashboard/`). The app downloads the
   release (or uses `HP_DB_PATH`) and reads the derived tables read-only.
