# Known Issues

v1 issues are archived in [`legacy/known_issues_v1.md`](legacy/known_issues_v1.md).

## Buy/wait signal not published yet

The gate rejects the current model (Brier skill −0.42 against the baseline on the
legacy history, see ADR-06), so products show "Señal no disponible". This is
expected until there are a few months of clean v2 history.

## Legacy history has unknown stock

Periods imported from v1 have no reliable stock information and cover fewer
stores before August 2026. Charts and the backtest treat them separately.

## Conservative grouping leaves some offers apart

Listings whose line is not in `configs/taxonomy.yaml` become products of their
own inside the right family. Review `qa-report` and extend the taxonomy and
the gold set when a store's naming is new.

## Newer categories: limits of the parsers

- Case family = model: two cases with the same model and different colors are one family.
  Cases and air coolers whose model is not in `configs/taxonomy.yaml` stay `unparsed`;
  extend `case_models` / `cooler_models` from `qa-report`.
- LGA1700 boards (600/700 chipsets) exist in DDR4 and DDR5. Without "D4"/"DDR4" in the
  title the DDR stays empty, the product is kept apart and the build asks to confirm it.
- Monitors without a declared panel form their own family and never merge.
- Gripper, Urubox and Aerobox do not publish a volumetric rule; monitors and cases only get
  a "box is large for its weight" warning.

## Banifox stock is unknown

The nopCommerce listing pages do not show availability, so Banifox offers have
`in_stock = NULL` and count as available.

## Import calculator: assumptions to keep an eye on

- Courier tariffs change without notice; Aerobox's published tariff dates from 2022-08.
  Gripper and Urubox do not publish weight rounding or volumetric rules (100 g steps and
  no volume charge are assumed; the calculator says so).
- Package weights per family are estimates until the user or the bookmarklet provides
  the real ones.
- Third-party sellers on Amazon/Newegg are computed with IVA (`uncertain` registry status):
  their invoices are not covered by the marketplace's registry entry.
- The franchise cap of a person in the cart optimizer is checked on one combined invoice;
  separate invoices can differ by a cent of sales-tax rounding.
- Amazon may change its page layout and break the bookmarklet selectors; it falls back to
  asking for the price.

## Datacenter IPs may be blocked

If a store starts returning errors only from GitHub Actions, the planned fallback
is a self-hosted runner for that store.
