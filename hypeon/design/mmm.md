# MMM (Marketing Mix Modeling)

The product engine includes a **custom MMM** in `packages/mmm`: daily spend by channel (meta, google, bing, pinterest), adstock transform, log saturation, and Ridge regression. Coefficients and R² are stored in `mmm_results` and used for budget optimization and attribution-vs-MMM comparison.

## Current implementation

- **Input:** Daily spend per channel (from raw_*_ads tables), daily revenue (Shopify + WooCommerce orders).
- **Transforms:** Adstock (configurable half-life), log saturation.
- **Model:** Ridge regression; optional bootstrap for confidence intervals.
- **Output:** Per-channel coefficients, goodness-of-fit R², used by optimizer and report.

## Future: Meridian, Robyn, or other MMMs

Google **Meridian** and Meta **Robyn** (or other MMM tools) can be integrated by replacing or complementing the current Ridge pipeline. The contract remains:

- **Input:** Daily time series of spend by channel and revenue (same as today).
- **Output:** Per-channel contribution or coefficients that can be used for budget allocation and attribution comparison.

To integrate an external MMM:

1. Run the external model on the same inputs (e.g. export spend/revenue from our raw/aggregated tables).
2. Map its output (e.g. channel coefficients or contributions) into `mmm_results` rows (run_id, channel, coefficient, goodness_of_fit_r2, ...) so the existing optimizer and report keep working.

The current custom MMM remains the default for initial stages.
