ATTACH 'SQL/L2_business_insights.duckdb' AS l2;
USE l2;
SELECT
    month,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(AVG(sales), 2) AS avg_sales_per_month,
    ROUND(VAR_POP(sales), 2) AS sales_variance,
    ROUND(STDDEV_POP(sales), 2) AS sales_stddev,
    COUNT(DISTINCT product_key) AS active_products
FROM l2.sales_join() AS f
GROUP BY month
ORDER BY month;