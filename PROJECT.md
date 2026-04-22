# Project Framing

## Problem

PC hardware prices in Uruguay exhibit high dispersion due to the small market size, fragmented retail channels, import costs, and exchange rate volatility. As a result, identical PC components are often listed at significantly different prices across retailers and marketplaces. Consumers and resellers have limited tools to determine whether a given listing represents a fair price relative to historical trends or the broader market.
This project aims to build a price intelligence system that aggregates listings from multiple sources, resolves product identities across stores, and models price dynamics in order to estimate fair prices and forecast future price movements.

---

## ML Task

Regression.
Predict the expected price of a canonical product SKU at horizon **H** using historical price data and external signals.

---

## Prediction Unit

A **canonical product SKU aggregated at weekly resolution**.
Each prediction estimates the expected market price for a specific SKU in a given week.
Example unit:
RTX 4070 Super
Week t + H

---

## Target Variable

The **median market price (USD) of a canonical SKU across retailers during a given week**.
Median is used to reduce the impact of outliers and extreme marketplace listings.

---

## Primary Metric

MAPE (Mean Absolute Percentage Error)
MAPE is appropriate for price prediction tasks because it expresses error as a percentage relative to the true price.

---

## Business Metric

The model is useful if it can:

- identify listings priced **significantly below expected market value**
- improve deal detection compared to naive historical averages
- provide reliable price forecasts for upcoming weeks
  Success criterion:
  The model consistently identifies **top 10–15% underpriced listings** with low false positives.

---

## Constraints

- Batch inference (daily)
- Interpretability required (SHAP explanations preferred over black-box models)
- Sparse time series for some SKUs
- Noisy product titles requiring entity resolution
- Data collected via scraping may be incomplete or irregular
- MercadoLibre not available in MVP (API geo-restricted, scraping blocked)