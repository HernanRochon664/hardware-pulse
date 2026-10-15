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

## Banifox stock is unknown

The nopCommerce listing pages do not show availability, so Banifox offers have
`in_stock = NULL` and count as available.

## Datacenter IPs may be blocked

If a store starts returning errors only from GitHub Actions, the planned fallback
is a self-hosted runner for that store.

## `src/ml/pipeline.py` is an unused stub

The ML orchestration lives in `src/ml/signals.py`. The old stub should be deleted.
