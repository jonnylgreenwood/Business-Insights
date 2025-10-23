ATTACH 'SQL/L2_business_insights.duckdb' AS l2;
USE l2;
SELECT
    cat_id,
    year,
    SUM(f.sales) AS total_sales,
    SUM(f.sales_value) AS total_sales_value,
    AVG(f.sales) AS avg_sales_per_year,
    COUNT(DISTINCT f.product_key) AS active_products
FROM l2.sales_join() AS f
GROUP BY cat_id, year
ORDER BY cat_id, year;