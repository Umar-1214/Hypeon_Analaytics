# Ingest contract: data/raw CSV schema

When the real data pipeline is integrated, it should produce (or you should write) the same CSVs into `DATA_RAW_DIR` (default `data/raw/`). The ingest layer in `packages/shared/src/ingest.py` reads these files and upserts into the raw tables. Alternatively, replace the CSV loaders with your own connector (S3, GCS, API) that populates the same raw tables.

## Ad platforms (daily aggregates)

| File | Columns | Notes |
|------|---------|--------|
| `meta_ads.csv` | date, campaign_id, campaign_name, spend, impressions, clicks | Upsert key: (date, campaign_id) |
| `google_ads.csv` | same | Same schema |
| `bing_ads.csv` | same | Same schema |
| `pinterest_ads.csv` | same | Same schema |

## Orders

| File | Columns | Notes |
|------|---------|--------|
| `shopify_orders.csv` | order_id, name, order_date, revenue, is_new_customer, [optional: click_id, utm_source, utm_medium, utm_campaign, net_revenue, ...] | order_id unique. click_id used for click-ID attribution. |
| `shopify_transactions.csv` | order_id, kind, status, amount, currency, created_at, gateway, parent_id, source_name | order_id = Shopify order_id; linked to raw_shopify_orders.id for reconciliation. |
| `woocommerce_orders.csv` | order_id, name, order_date, revenue, is_new_customer, [optional: click_id, utm_*, net_revenue] | Same shape as Shopify for unified attribution/MMM. |

## Click-ID attribution

| File | Columns | Notes |
|------|---------|--------|
| `ad_clicks.csv` | click_id, date, campaign_id, campaign_name, channel | channel: meta, google, bing, pinterest. Orders with matching click_id get 100% revenue attributed to that channel/campaign. |

## Run order

`run_ingest()` loads in this order: meta_ads, google_ads, bing_ads, pinterest_ads, shopify_orders, shopify_transactions, woocommerce_orders, ad_clicks; then reconciles Shopify order net_revenue from transactions.
