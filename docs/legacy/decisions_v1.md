# Architecture Decisions

## PC Store Uruguay — discarded from MVP

PC Store uses Cloudflare + dynamic CSRF tokens per session.
Reliable scraping would require complex session handling and is
potentially fragile to Cloudflare changes.

Decision: exclude from MVP. Initial dataset covers:

- Thot Computación (static WooCommerce)
- Banifox (internal JSON endpoint)

It can be reviewed in future iterations with Playwright + session handling.

## PCcompu — included in MVP

PCcompu uses server-rendered HTML with simple pagination via ?pagina=N query parameter.
No Cloudflare or complex session handling required.

Decision: include in MVP. Dataset now covers Thot + Banifox + PCcompu.

## MercadoLibre — excluded from MVP

The MercadoLibre API returns 403 (PolicyAgent) for all endpoints from
Uruguay, regardless of authentication. Web scraping with Playwright is
also blocked: ML detects automated sessions and redirects to login after
the first page navigation.

Decision: exclude ML from MVP. Dataset covers Thot + Banifox + PCcompu.
Future option: playwright-stealth, proxy rotation, or authenticated session.

## FX Conversion — moved to resolve pipeline

Currency normalization (UYU → USD) was initially spread across storage
and resolve layers. It was consolidated into the resolve pipeline to
keep a single source of truth for price normalization.

Decision: `src/pipelines/fx.py` contains `normalize_to_usd()` and
`fetch_fx_for_week()`. The resolve pipeline calls these when processing
UYU listings. Storage layer stores only already-normalized prices.

## Entity Resolution Strategy Priority

Three strategies are applied in strict order: exact → regex → fuzzy.
Exact match (confidence 1.0) takes priority to avoid false positives
from regex over-matching. Fuzzy is the last resort with a 0.8 threshold.

Decision: prioritized exact match for known catalog patterns. Regex
extracts structural patterns (e.g., RTX 4070 Ti). Fuzzy catches listing
variations with minor word reordering.

## Model Selection — ElasticNet vs NaivePersistence

Two models are trained per SKU:
- **ElasticNet** — linear model with L1/L2 regularization, good for
  sparse feature sets with FX rate and lag features
- **NaivePersistence** — predicts next week = last observed value,
  strong baseline for stable prices

The winner (lowest MAPE) is persisted per training run. This avoids
over-committing to a single model architecture on a small dataset.

Decision: train both, pick winner by MAPE. Artifact stores which model
won and its metadata for auditability.

## Prediction Confidence Bounds

Inference produces a 90%/110% interval around the point prediction.
These are fixed-width bounds (not statistical prediction intervals)
chosen for simplicity in the MVP.

Decision: fixed 90-110% range. Future iterations may use conformal
prediction or quantile regression for adaptive bounds.

## Weekly Aggregation

All price snapshots are aggregated to weekly buckets (Monday UTC).
This matches the typical pricing update cadence of Uruguayan retailers
and reduces noise from daily fluctuations.

Decision: weekly aggregation with Monday-aligned week starts.

## Minimum History for Training

SKUs with fewer than 12 weeks of price history are excluded from
training. This ensures models have enough signal to learn seasonal
and trend patterns.

Decision: 12-week minimum. Tuneable via `MIN_WEEKS` in training script.

## Deal Threshold Configurable

The deal detection threshold is configurable via a Streamlit sidebar
slider (1-50%, default 10%). This allows users to adjust sensitivity
without code changes.

Decision: runtime configuration via UI slider, not hardcoded constant.
