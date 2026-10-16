# ¿Comprar acá o importar? — reglas, fórmulas y mantenimiento

The import calculator (`src/crossborder/`, page "¿Comprar acá o importar?") compares the
best local price with the landed cost of buying in a US store and shipping through a
courier. This document records the rules it implements, where each number comes from
and how to keep them up to date. Rules verified on **2026-10-16**.

## Rules (Ley 20.446 arts. 627-635, Decreto 50/026, in force since 2026-05-01)

| Topic | Rule | Source |
|---|---|---|
| Shipment value | Total of the original invoice with every charge on it: price, US sales tax, the store's shipping to the warehouse. The courier's freight is **not** part of it. | Decreto 50/026 art. 5 |
| Franchise | Up to 3 shipments per calendar year and USD 800 in total per adult with a Uruguayan ID, personal use, paid with an international card in the name of the buyer and recipient. Counted on the release date; shipments of January-April 2026 count. | Decreto 50/026 arts. 3, 4, 18 |
| IVA under the franchise | 22% of the invoice value, **minimum USD 20 per shipment** unless every good is exempt. Exempt from tariffs. | TO 2023 Título 10 art. 13 lit. B inc. 3 (added by Ley 20.446 art. 660) |
| US exemption (TIFA) | No tax when the shipment is sent by a seller with US fiscal residence, every item qualifies and the invoice is at most USD 200. It still uses one shipment and counts toward the USD 800. From **2026-11-03** the invoice issuer must be in the DNA registry. | Decreto 50/026 art. 3; [Aduanas](https://www.aduanas.gub.uy/innovaportal/v/28221/1/innova.front/) |
| DNA registry | Amazon.com Inc., Newegg, B&H, Best Buy, Micro Center, eBay, Walmart, Tiendamia (Xipron), Grinbox, Netbox, Miami Box, USX, Eshop Miami, pedido.miami… (list of 2026-10-15). A third-party seller on Amazon is **not** covered by Amazon's entry. | [PDF](https://www.aduanas.gub.uy/innovaportal/file/28873/1/control.empresas.pdf) |
| Single tax | 60% of the invoice value, minimum USD 20, any number of shipments, replaces every other import tax. | Decreto 50/026 art. 2 |
| Limits | Both regimes: invoice ≤ USD 800 and ≤ 20 kg. Above that it is a formal import with a customs broker (not priced). | Decreto 50/026 arts. 1-3 |
| Regimes never combine | A shipment that does not meet the franchise conditions (e.g. not enough balance left) goes **entirely** to the 60% regime. | Decreto 50/026 art. 15 |
| Penalties | Inaccurate declared value: fine of twice the taxes and loss of the franchise. | Ley 20.446 art. 632 |
| TFSPU | 10% of the courier's service price, IVA excluded. | [Ley 19.009 art. 15](https://www.impo.com.uy/bases/leyes/19009-2012) |
| Sales tax | 7% in Miami-Dade (6% state + 1% county). Removed by a forwarding agent certificate (DR-14 FAA) registered at the store (Grinbox offers one; Amazon ATEP, eBay, Best Buy, Newegg, B&H). | Avalara; [Grinbox](https://www.grinbox.uy/tax-free) |

## Formula of one shipment

```
V  = Σ price × qty + store shipping + sales tax (+ assisted-purchase fee)      invoice value
   + card fee        = rate × V × 1.22 (when the card charges IVA on it)
   + freight         = flat band (real weight) or per-kg band × weight rounded up to the step
   + volume surcharge  (couriers that publish it)
   + handling        (+ 22% IVA when the courier says "+ IVA")
   + TFSPU           = 10% × (freight [+ handling])
   + insurance / delivery (optional, when published)
   + Uruguayan tax   = 0 (US exemption) | max(22% × V, 20) (franchise) | max(60% × V, 20)
```

Every line is rounded to cents and the total is the sum of the lines. The cheapest
courier × regime wins; the "value of the franchise" is the 60% total minus the franchise
total. The **target price** is found by bisection: the landed cost never decreases when the
price goes up, also across the jump at USD 200.

## Cart optimizer

For each courier (one account per plan) the optimizer searches every way to split the
units into local purchases, 60% shipments and franchise shipments of each person, with
dynamic programming over subsets (O(3ⁿ), n ≤ 10). The franchise cap of a person is checked
on the invoice of all their shipments together (they may differ by a cent of sales-tax
rounding; the plan warns when it is that close). `tests/test_crossborder_optimizer.py`
compares it with a brute force on random carts.

## What is not in the total

Warranty (RMA to the US), returns (international freight, taxes not refunded), delivery
time, loss or damage and the requirements are listed apart. The verdict asks for a minimum
saving (default: the larger of USD 25 and 10%) before saying "conviene importar".

## Maintenance

`.github/workflows/freshness.yml` runs on the 1st of each month (and on demand): when any
tariff, rule set or the registry was checked more than 60 days ago, it opens or updates the
issue "Revisar tarifas y reglas de la calculadora de importación" with a checklist
(`python -m src.crossborder.freshness` prints the same locally). Close the issue after
updating the files.

- **Couriers** (`configs/couriers.yaml`): re-check the official tariff pages every couple of
  months, update the numbers and `verified_at`. The calculator warns after 60 days. Only
  couriers with a published tariff are listed.
- **Rules** (`configs/import_rules.yaml`): when the law changes, add a new rule set with its
  `valid_from` instead of editing the old one.
- **Registry** (`configs/us_stores.yaml`): download the DNA PDF and update `registry_status`
  and `registry.checked_at`.
- **Package profiles** (`configs/shipping_profiles.yaml`): estimates; the user can edit them
  and the bookmarklet brings the real weight and size when the store page shows them.
- **Bookmarklet** (`src/dashboard/static/bookmarklet.js`): when a store changes its page,
  update the selectors and the trimmed pages in `tests/fixtures/us_pages/`.
