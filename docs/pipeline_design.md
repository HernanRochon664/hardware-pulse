# Pipeline Design

## 1. Input Data Contract

Each raw listing contains:

| field                | type     | source       | notes                                           |
| -------------------- | -------- | ------------ | ----------------------------------------------- |
| title                | str      | all          | raw product name                                |
| price                | float    | all          | listing price                                   |
| currency             | str      | all          | UYU or USD                                      |
| seller               | str      | all          | seller name or store                            |
| url                  | str      | all          | canonical product URL                           |
| timestamp            | datetime | system       | scrape time                                     |
| source               | str      | system       | e.g. "mercadolibre", "thot"                     |
| item_id              | str      | mercadolibre | MercadoLibre item identifier (MLU...)           |
| sku_guess            | str      | pipeline     | extracted GPU model (e.g. RTX 4070)             |
| brand                | str      | pipeline     | extracted brand (Asus, MSI, Gigabyte, etc.)     |
| model_variant        | str      | pipeline     | optional AIB model (e.g. TUF, Gaming X, Ventus) |
| condition            | str      | mercadolibre | new / used (when available)                     |
| available_quantity   | int      | mercadolibre | listing stock                                   |
| base_price           | float    | mercadolibre | price before discounts (if present)             |
| normalized_price_usd | float    | pipeline     | converted price in USD for comparison           |

Notes:

- Some fields are only available from certain sources (e.g., item_id from MercadoLibre).
- Fields such as sku_guess, brand, and model_variant are extracted during normalization.

---

## 2. Pipeline Steps

The pipeline follows a linear ingestion and transformation process designed to build a time-series dataset of GPU prices.

### 1. Scrape

The pipeline collects listings from multiple sources.

Sources include:

- MercadoLibre API
- Thot Computación
- Banifox
- PC Store

For MercadoLibre:

1. Query the search endpoint:

GET https://api.mercadolibre.com/sites/MLU/search?q=rtx+4070

2. Extract listing IDs and metadata.

3. Fetch detailed item data:

GET https://api.mercadolibre.com/items/{ITEM_ID}

Important fields returned include:

- price
- base_price
- currency_id
- available_quantity

Because the API does not provide historical prices, the system captures periodic snapshots.

For retail stores (Thot, Banifox, PC Store):

- HTTP requests using requests
- HTML parsing with BeautifulSoup
- Extract title, price, and product URL

---

### 2. Normalize

Listings from different sources are normalized into a unified schema.

Normalization includes:

- Currency normalization (UYU → USD)
- Cleaning product titles
- Removing noise such as:
    - “12 cuotas”
    - “envío gratis”
    - promo tags

Extracted attributes:

- brand
- gpu_model
- variant

Example:

"ASUS TUF RTX 4070 OC 12GB GDDR6X"

→

brand: ASUS  
gpu_model: RTX 4070  
variant: TUF OC

---

### 3. Match (Entity Resolution)

Multiple listings referring to the same GPU model must be grouped together.

Matching strategy (in order of priority):

1. Regex + canonical catalog  →  primary strategy
2. Fuzzy matching             →  fallback if regex fails
3. No match                   →  flagged for manual review

Example canonical SKUs:

RTX 4060  
RTX 4060 Ti  
RTX 4070  
RX 7800 XT  
RX 7900 XTX

Example listings:

- MSI Gaming X RTX 4070
- ASUS Dual RTX 4070 OC
- Zotac RTX 4070 Twin Edge

→ mapped to canonical_product = RTX 4070

---

### 4. Store

The pipeline stores both:

- raw scraped data
- normalized price snapshots

Storage is append-only for price snapshots to preserve historical integrity.

---

### 5. Analyze

After sufficient data collection, analysis tasks include:

- Price tracking
- Vendor comparisons
- Minimum price detection
- Trend detection
- Forecasting

Potential forecasting models:

- naive baseline
- ARIMA
- Prophet
- regression-based models

---

## 3. Data Processing Rules

### Currency Handling

All prices are normalized to USD.

Exchange rates are retrieved daily from the Frankfurter API.

Example endpoint:

https://api.frankfurter.app/latest?from=USD&to=UYU

Exchange rates are stored historically in the database to ensure
reproducibility of price normalization.

normalized_price_usd =

    if currency == "USD":
        price
    else:
        price / exchange_rate

---

### Duplicate listings

MercadoLibre provides a stable listing identifier (item_id).

Primary deduplication strategy:

UNIQUE(item_id, timestamp)

This allows tracking price changes of the same listing over time.

Seller reposts with new item_id are treated as independent listings
and resolved during aggregation (e.g. min price per product per day).

---

### Used vs new GPUs

The primary forecasting target focuses on the retail market
for new hardware.

Therefore:

condition = new is used for the main price series.

Used listings are still stored but analyzed separately.

Rationale:

- used GPUs have large variance
- hardware condition is unknown
- mining usage introduces strong noise
- mixing used and new would corrupt the time series

---

## 4. Storage Design

Engine: SQLite

Reasons:

- Dataset expected to stay below ~100k rows
- No operational overhead
- Native Python support
- Easy migration to PostgreSQL later if required

### Tables

#### raw_listings

Stores raw scraped listings exactly as collected.

Fields:

id  
source  
item_id  
title  
price  
currency  
seller  
url  
timestamp  
raw_payload

Purpose:

- debugging
- reproducibility
- reprocessing when pipeline logic changes

---

#### canonical_products

Defines the catalog of GPUs tracked by the system.

Example entries:

RTX 4060  
RTX 4060 Ti  
RTX 4070  
RTX 4080  
RX 7800 XT  
RX 7900 XTX

Fields:

id  
brand_family (NVIDIA / AMD)  
model  
release_year  
notes

---

#### price_snapshots

Core time-series table.

Each row represents the price of a listing at a specific timestamp.

Fields:

id  
timestamp  
canonical_product_id  
source  
seller  
listing_id  
price  
currency  
price_usd  
availability

Example:

| timestamp  | product  | seller   | price |
| ---------- | -------- | -------- | ----- |
| 2026-03-01 | RTX 4070 | Seller A | 749   |
| 2026-03-02 | RTX 4070 | Seller A | 729   |
| 2026-03-03 | RTX 4070 | Seller A | 729   |

---

## 5. Source Inventory

| source       | tool                     | js_required | approx_listings | difficulty |
| ------------ | ------------------------ | ----------- | --------------- | ---------- |
| MercadoLibre | API (requests)           | No          | ~200            | Medium     |
| Thot         | requests + BeautifulSoup | No          | ~15–30          | Very Low   |
| Banifox      | requests + BeautifulSoup | No          | ~30–35          | Low        |
| PC Store     | requests + BeautifulSoup | No          | ~40–80          | Low–Medium |

Notes:

MercadoLibre uses the public API:

/sites/MLU/search?q=rtx+4070

Requires pagination handling and contains both new and used listings.

Thot uses WooCommerce and static HTML with pagination like /shop/page/N.

Banifox pages are server-rendered HTML categories.

PC Store has a slightly larger catalog and may include used items.

---

## 6. Entity Resolution Strategy

### Resolution pipeline

Strategies are applied in order until a match is found.
Unresolved listings are flagged and excluded from price_snapshots
until manually reviewed.

The resolution strategy combines three approaches.

### 1. Canonical catalog

Manually curated GPU models:

RTX 3050  
RTX 3060  
RTX 4060  
RTX 4060 Ti  
RTX 4070  
RTX 4070 Super  
RTX 4070 Ti  
RTX 4080  
RTX 4090

RX 6600  
RX 6650XT  
RX 7600  
RX 7700XT  
RX 7800XT  
RX 7900XT  
RX 7900XTX

Approx. 15–20 base models.

---

### 2. Attribute extraction

Titles parsed using:

- regex rules
- token normalization
- brand dictionaries

Example:

"MSI RTX4070 Gaming X Trio"

→

brand = MSI  
gpu_model = RTX 4070  
variant = Gaming X Trio

---

### 3. Fuzzy matching

Fallback strategy when extraction fails.

Possible libraries:

- rapidfuzz
- fuzzywuzzy

Used to map slightly different titles to canonical SKUs.

---

## 7. Open Questions

### MercadoLibre API limits

- Are there strict rate limits?
- Is authentication required for heavy usage?
- How stable are search results across repeated queries?

---

### Scraping frequency

Current assumption:

1 scrape per day

Alternative:

4 scrapes per day

Trade-off:

- higher frequency → better time resolution
- higher frequency → more duplicated observations

---

### Product variants

Variants appear such as:

RTX 4070  
RTX 4070 OC  
RTX 4070 Ti  
RTX 4070 Super

Decision required:

- treat as separate canonical products
- or aggregate under base SKU
