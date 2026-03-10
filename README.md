# Hardware Pulse

A data pipeline that tracks and analyzes price dynamics of PC components in the Uruguayan electronics market.
The system collects product listings from local retailers and marketplaces, resolves product identities across stores, and builds a historical price dataset to detect pricing inefficiencies and forecast optimal buying windows.

---

# Motivation

PC hardware prices in Uruguay show high dispersion due to:

- small market size
- fragmented retail channels
- import costs
- exchange rate fluctuations
  Consumers and resellers often struggle to know whether a listing represents a good deal.
  This project builds a **price intelligence system** that detects how global events (product launches, FX changes, promotions) propagate into local prices.

---

# Scope (MVP)

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

# Data Sources

The system collects listings from:

### Marketplaces
- MercadoLibre

### Local retailers
- Thot Computación
- PC Store Uruguay

Each listing contains:

- title
- price
- currency
- seller
- timestamp
- product URL

---

# Architecture

High-level pipeline:
Scrapers → Raw Storage → Product Matching → Price History → Analytics
Pipeline stages:

1. Web scraping
2. Product normalization
3. Entity resolution
4. Price history storage
5. Analytics / forecasting

---

# Product Matching

Product names vary significantly between stores.
Example:
RTX 4070 Super ASUS Dual
ASUS Dual RTX4070S 12GB OC
Placa de Video ASUS 4070 SUPER 12G
To resolve duplicates, the system extracts:

- brand
- chipset
- memory
- variant
  Matching pipeline:

1. Normalize text
2. Extract GPU/CPU model
3. Generate embeddings
4. Nearest-neighbor matching
   This produces a **canonical product graph**.

---

# Example Insights

The dataset enables analyses such as:

- price dispersion across stores
- price convergence after launches
- arbitrage opportunities between Amazon and local retailers
- seasonal price patterns

---

# Example Use Case

A user wants to buy a GPU.
Instead of checking dozens of listings manually, the system shows:

- historical price distribution
- current price percentile
- expected price window
  Example:
  RTX 4070 Super
  Current price: $720
  Historical median: $780
  Signal: **Good deal**

---

# Tech Stack

Python

### Scraping

- requests
- beautifulsoup
- playwright

### Data

- pandas
- PostgreSQL

### ML / NLP

- sentence-transformers
- scikit-learn

### Visualization

- matplotlib
- streamlit

---

# Limitations

- Scraping reliability depends on website structure
- Some listings lack structured product metadata
- Matching errors may occur for niche SKUs

---

# Roadmap

### Phase 1 — Data Pipeline (MVP)

- Scrapers for major Uruguayan retailers
- Product normalization
- Entity resolution for canonical SKUs
- Historical price dataset

### Phase 2 — Market Analytics

- Price dispersion analysis
- Historical price distribution per SKU
- Cross-store comparison
- Launch price tracking

### Phase 3 — Predictive Modeling

- Price forecasting models
- Integration of FX and global price signals
- Detection of abnormal pricing

### Phase 4 — User Layer

- Deal detection system
- Price alerts
- Streamlit dashboard
- API for price queries

---

# License

MIT