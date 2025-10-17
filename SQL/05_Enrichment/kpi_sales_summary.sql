-- CREATE OR REPLACE TABLE l2.kpi_sales_summary AS
SELECT
    st.store_key,
    c.wm_yr_wk AS week,
    SUM(f.sales) AS total_sales,
    AVG(f.sales) AS avg_sales_per_day,
    COUNT(DISTINCT f.product_key) AS active_products
FROM l2_sales_long AS l2
LEFT JOIN dim_calendar AS c USING (d)
LEFT JOIN dim_store AS st USING (store_key)
GROUP BY st.store_key, c.wm_yr_wk
LIMIT 20;