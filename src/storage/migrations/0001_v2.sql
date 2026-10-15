-- Hardware Pulse v2 schema.
-- Timestamps: UTC ISO-8601 strings ending in "Z". Dates: local (UYT) YYYY-MM-DD.

CREATE TABLE stores (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    base_url    TEXT NOT NULL,
    platform    TEXT NOT NULL
);

-- One row per (pipeline run, store, category) scrape attempt.
CREATE TABLE scrape_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT NOT NULL,
    store_id    TEXT NOT NULL REFERENCES stores(id),
    category    TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    status      TEXT NOT NULL
                CHECK (status IN ('running', 'ok', 'empty', 'anomaly', 'error')),
    n_found     INTEGER NOT NULL DEFAULT 0,
    n_new       INTEGER NOT NULL DEFAULT 0,
    n_changed   INTEGER NOT NULL DEFAULT 0,
    n_delisted  INTEGER NOT NULL DEFAULT 0,
    error       TEXT
);
CREATE INDEX idx_scrape_runs_store_cat ON scrape_runs (store_id, category, started_at);
CREATE INDEX idx_scrape_runs_run ON scrape_runs (run_id);

-- Market-level grouping: GPU chip + VRAM, CPU model, RAM spec, SSD class.
CREATE TABLE families (
    id          TEXT PRIMARY KEY,
    category    TEXT NOT NULL,
    name        TEXT NOT NULL,
    specs_json  TEXT NOT NULL DEFAULT '{}'
);

-- Exact product: what a buyer actually orders, shared across stores.
CREATE TABLE products (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL UNIQUE,
    category    TEXT NOT NULL,
    family_id   TEXT REFERENCES families(id),
    brand       TEXT,
    line        TEXT,
    name        TEXT NOT NULL,
    mpn         TEXT,
    specs_json  TEXT NOT NULL DEFAULT '{}',
    image_url   TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
CREATE INDEX idx_products_family ON products (family_id);
CREATE INDEX idx_products_mpn ON products (mpn);

-- A store's product page (offer). Latest descriptive data lives here; the
-- price history lives in price_periods.
CREATE TABLE listings (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id         TEXT NOT NULL REFERENCES stores(id),
    url              TEXT NOT NULL,
    store_sku        TEXT,
    title            TEXT NOT NULL,
    category         TEXT NOT NULL,
    brand            TEXT,
    image_url        TEXT,
    first_seen_at    TEXT NOT NULL,
    last_seen_at     TEXT NOT NULL,
    is_active        INTEGER NOT NULL DEFAULT 1,
    condition        TEXT NOT NULL DEFAULT 'new',
    mpn              TEXT,
    product_id       INTEGER REFERENCES products(id),
    match_status     TEXT,
    match_notes      TEXT,
    resolver_version TEXT,
    resolved_title   TEXT,
    UNIQUE (store_id, url)
);
CREATE INDEX idx_listings_product ON listings (product_id);
CREATE INDEX idx_listings_store_cat ON listings (store_id, category, is_active);

-- Change-based price history: a period is extended while the price, regular
-- price and stock state stay the same and the listing stays published.
CREATE TABLE price_periods (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id    INTEGER NOT NULL REFERENCES listings(id) ON DELETE CASCADE,
    price         REAL NOT NULL,
    regular_price REAL,
    currency      TEXT NOT NULL,
    price_usd     REAL NOT NULL,
    in_stock      INTEGER,
    valid_from    TEXT NOT NULL,
    last_seen     TEXT NOT NULL,
    origin        TEXT NOT NULL DEFAULT 'scrape' CHECK (origin IN ('scrape', 'legacy'))
);
CREATE INDEX idx_periods_listing ON price_periods (listing_id, valid_from);
CREATE INDEX idx_periods_last_seen ON price_periods (last_seen);

CREATE TABLE fx_rates (
    date        TEXT PRIMARY KEY,
    usd_uyu     REAL NOT NULL,
    source      TEXT NOT NULL,
    fetched_at  TEXT NOT NULL
);

-- ---------------------------------------------------------------------------
-- Derived tables, rebuilt by the aggregate stage.
-- ---------------------------------------------------------------------------

CREATE TABLE quality_issues (
    listing_id  INTEGER PRIMARY KEY REFERENCES listings(id) ON DELETE CASCADE,
    kind        TEXT NOT NULL,
    detail      TEXT,
    detected_at TEXT NOT NULL
);

CREATE TABLE product_daily (
    product_id  INTEGER NOT NULL,
    date        TEXT NOT NULL,
    min_usd     REAL NOT NULL,
    n_offers    INTEGER NOT NULL,
    n_stores    INTEGER NOT NULL,
    PRIMARY KEY (product_id, date)
) WITHOUT ROWID;

CREATE TABLE family_daily (
    family_id   TEXT NOT NULL,
    date        TEXT NOT NULL,
    min_usd     REAL NOT NULL,
    p25_usd     REAL NOT NULL,
    median_usd  REAL NOT NULL,
    n_products  INTEGER NOT NULL,
    PRIMARY KEY (family_id, date)
) WITHOUT ROWID;

CREATE TABLE product_stats (
    product_id        INTEGER PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
    best_price_usd    REAL,
    best_listing_id   INTEGER,
    best_store_id     TEXT,
    best_regular_usd  REAL,
    n_offers          INTEGER NOT NULL DEFAULT 0,
    n_stores          INTEGER NOT NULL DEFAULT 0,
    min_180d          REAL,
    max_180d          REAL,
    change_7d         REAL,
    change_30d        REAL,
    last_change_at    TEXT,
    is_historic_low   INTEGER NOT NULL DEFAULT 0,
    history_days      INTEGER NOT NULL DEFAULT 0,
    updated_at        TEXT NOT NULL
);

CREATE TABLE model_registry (
    version      TEXT PRIMARY KEY,
    trained_at   TEXT NOT NULL,
    kind         TEXT NOT NULL,
    params_json  TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    is_active    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE signals (
    entity_type   TEXT NOT NULL CHECK (entity_type IN ('product', 'family')),
    entity_id     TEXT NOT NULL,
    as_of         TEXT NOT NULL,
    horizon_days  INTEGER NOT NULL,
    p_drop        REAL NOT NULL,
    recommendation TEXT NOT NULL,
    model_version TEXT NOT NULL,
    details_json  TEXT,
    PRIMARY KEY (entity_type, entity_id)
);
