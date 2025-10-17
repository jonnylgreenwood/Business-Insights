ATTACH 'SQL/L2_forecast_accuracy.duckdb' AS l2;
USE l2;

WITH monthly_sales AS (
    SELECT 
        product_key,
        month,
        SUM(s.sales_value) AS total_sales
    FROM l2.sales_join() s
    GROUP BY product_key, month
),
seasonality_stats AS (
    SELECT 
        product_key,
        STDDEV_POP(total_sales) / NULLIF(AVG(total_sales),0) AS seasonality_strength
    FROM monthly_sales
    GROUP BY product_key
),
ranked AS (
    SELECT
        product_key,
        seasonality_strength,
        NTILE(5) OVER (ORDER BY seasonality_strength DESC) AS seasonality_band
    FROM seasonality_stats
)
SELECT 
    *,
    CASE seasonality_band
        WHEN 1 THEN 'Top 20% Seasonality'
        WHEN 2 THEN 'Top 20–40% Seasonality'
        WHEN 3 THEN 'Top 40–60% Seasonality'
        WHEN 4 THEN 'Top 60–80% Seasonality'
        WHEN 5 THEN 'Top 80-100% Seasonality'
    END AS seasonality_label
FROM ranked;