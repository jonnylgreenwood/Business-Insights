ATTACH 'SQL/L1_forecast_accuracy.duckdb' AS l1 (READ_ONLY);

-- 1. Base sales fact
CREATE OR REPLACE TABLE fact_sales AS
SELECT
    product_id,
    store_id,
    date,
    sales
FROM l1.fact_sales_long
WHERE sales IS NOT NULL;

-- 2. Enrich with prices and calendar
CREATE OR REPLACE TABLE fact_sales_enriched AS
SELECT
    s.product_id,
    s.store_id,
    c.date,
    c.wm_yr_wk,
    c.weekday,
    c.month,
    c.year,
    p.sell_price,
    s.sales
FROM fact_sales s
LEFT JOIN l1.sell_prices p
  ON p.item_id = s.product_id AND p.store_id = s.store_id AND p.wm_yr_wk = c.wm_yr_wk
LEFT JOIN l1.dim_calendar c
  ON s.date = c.date;