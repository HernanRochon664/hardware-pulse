# Architecture Decisions (v2)

v1 decisions are archived in [`legacy/decisions_v1.md`](legacy/decisions_v1.md).

## ADR-01: Rewrite around the exact product

**Context.** The October 2026 audit found that v1 compared chips (the median of
every RTX 5060 of any brand), so price moves came from the stock mix and not from
the market. Its fuzzy matcher also merged wrong products (3080→5080,
5070 Ti→5070, SODIMM with DIMM).
**Decision.** The unit is the exact product (brand + line + specs + modifiers,
or the MPN), grouped into families for market views. Strict per-category
parsers replace fuzzy matching. When a listing cannot be identified with
confidence it stays a product of its own.
**Consequences.** Some offers that are really the same product stay apart,
but prices are never mixed. A gold set gates resolution in CI.

## ADR-02: Change-based price periods instead of daily snapshots

**Context.** v1 re-inserted every raw listing every day with its
first-seen date (72k snapshots from ~1k listings) and never recorded
"last seen", so delisted products kept their old price.
**Decision.** `price_periods(valid_from, last_seen)` per listing: extended while
nothing changes, a new period on any change. Listings are delisted only after
a successful run of their store and category. Runs with zero results or with
fewer than 50% of the usual count are `anomaly` and change nothing.
**Consequences.** Idempotent runs, compact history and step charts. A store outage
degrades the run (exit code 2) without wiping data.

## ADR-03: Three platform adapters for 10 stores

**Decision.** WooCommerce Store API (6 stores), the shared "SME" HTML platform
(3 stores, category URLs need the store suffix) and nopCommerce (Banifox).
Prices are parsed with explicit thousands/decimal separators: v1 read
"1.049" as USD 1.049.
**Consequences.** Adding a store on a known platform only needs config. Tests
run offline on trimmed real fixtures; `-m live` is an optional smoke test.

## ADR-04: Scraping in GitHub Actions, data in a Release

**Context.** v1 ran on a laptop scheduler and committed an 18 MB database
every day (`.git` reached 173 MB).
**Decision.** Cron at 09:00 and 17:00 UYT. The workflow downloads `data-latest`,
runs the pipeline and uploads the gzipped DB and `status.json` with `--clobber`,
plus a 30-day artifact backup. A monthly keepalive re-enables the schedule.
**Consequences.** No data in git. Degraded runs fail the job (email
notification). If a store blocks datacenter IPs, the fallback is a
self-hosted runner for that store.

## ADR-05: Streamlit, Spanish, multipage, read-only

**Decision.** `streamlit_app.py` with `st.navigation`: Inicio, Catálogo
(filters in `st.query_params`), Producto (`?p=<id>`), Familias, Estado de
datos and Señal. Read-only connection, cached queries over derived tables, and
Uruguayan formatting (`U$S 1.049`) with a USD/UYU toggle.
**Consequences.** Dollar signs must be escaped (`md()` / `html()` in
`formatting.py`) or Streamlit renders "U$S … $" as LaTeX. Charts use a
blue/orange/grey palette that is distinguishable in light and dark themes, and
badges always carry text, never color alone.

## ADR-06: Buy/wait signal with a publication gate

**Context.** v1 trained on 9 rows of one SKU with same-week leakage, and
its predictions were never shown.
**Decision.** Classification ("will the lowest price drop ≥ 5% in 14 days?") with
point-in-time features. Logistic regression versus a category × 30-day-median
baseline, evaluated walk-forward with a 14-day purge. The model is stored as JSON.
Signals are published only if Brier skill > 0 and PR-AUC > base rate, and only for
products with ≥ 21 days of history.
**Status (2026-10-15).** On the imported v1 history the model ranks better
than the baseline (PR-AUC 0.17 vs 0.11), but it is badly calibrated
(Brier skill −0.42), so the gate keeps it unpublished. Clipping features and
stronger regularization helped. Platt calibration and recent-window training
did not reach a positive skill. The model is retrained weekly and will publish
itself once clean v2 history makes it beat the baseline.

## ADR-08: "¿Comprar acá o importar?" as versioned rules and an exact optimizer

**Context.** The 2026 postal regime (Ley 20.446, Decreto 50/026) made importing a
calculation with traps: IVA with a USD 20 minimum, a USD 200 exemption that the US sales
tax can break, 3 shipments and USD 800 per year, and the 60% regime when the franchise
does not fit. Amazon closed its public API in 2026 and paid data sources were ruled out.
**Decision.** A pure engine (`src/crossborder/`) with the rules, courier tariffs, the DNA
registry and package profiles in YAML, each number with its source and check date. The
US price comes from the user, helped by search links and a bookmarklet that runs in the
user's browser (no scraping). Several products are planned by an exact subset DP that also
uses relatives' franchises. Risks (warranty, returns, delays) stay out of the total; the
verdict asks for a minimum saving instead. No schema change: everything is computed in
the dashboard. Details in [`importacion.md`](importacion.md).
**Consequences.** Tariffs and rules need a periodic manual check (the UI warns after 60
days). Without automatic US prices the catalog shows a target price per product instead of
"import saves X%".

## ADR-09: Five more categories, compatibility as warnings

**Context.** The import cart compared single parts only; a whole build (CPU + board +
cooler + RAM …) and monitors by variety could not be compared. 7 of the 9 active stores
publish motherboards, coolers, PSUs, cases and monitors with parseable titles.
**Decision.** Same pipeline and storage (no schema change), one strict parser per category,
each requiring a strong category mark (chipset, inches, wattage, a taxonomy model) so no
parser accepts another category's titles. Families: board = socket + chipset + format;
monitor = rounded size + resolution + Hz range + panel (exact Hz is a spec and a filter);
cooler = AIO radiator or air class; PSU = wattage + 80 Plus; case = model (a "light"
category). A store URL that shows up under two categories in one run is kept by the first
one in enum order, so GPU/CPU/RAM/SSD keep their listings; a listing whose category changes
is re-resolved. The build in the import page has one slot per category and
`src/entities/compat.py` reports socket, DDR, SODIMM, iGPU and board-format mismatches as
warnings, never as blocks: a missing spec means "not checked".
**Consequences.** The old 820 gold entries resolve exactly as before. Coverage of the
cooler and case taxonomies grows by hand from `qa-report`. New categories have no buy/wait
signal until they collect 21 days of history.

## ADR-07: Import the v1 history once, as `origin='legacy'`

**Decision.** `import-legacy` walks the git commits that contain the v1 DB,
rebuilds price periods, fixes the PCCompu ×1000 parsing bug and uses the v1
logs to avoid extending periods across store outages. Old Banifox URLs
resolve to the same products.
**Consequences.** Product history starts in April 2026. Stock in that period
is unknown, so the backtest reports the legacy and clean periods separately.
