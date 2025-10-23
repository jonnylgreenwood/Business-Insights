ATTACH 'SQL/L2_business_insights.duckdb' AS l2;
USE l2;

SELECT 
    product_key,
    AVG(sales_value) AS avg_sales,
    STDDEV_POP(sales_value) AS std_sales,
    STDDEV_POP(sales_value)/NULLIF(AVG(sales_value),0) AS coeff_var
FROM l2.l2_sales_long
GROUP BY product_key
HAVING coeff_var > 1;

CREATE OR REPLACE TABLE l2.sales_profile AS
SELECT
    product_key,
    COUNT(*) AS obs,
    AVG(sales_value) AS avg_sales,
    STDDEV_POP(sales_value) AS std_sales,
    STDDEV_POP(sales_value)/NULLIF(AVG(sales_value),0) AS coeff_var,
    coeff_var >= 1 AS coeff_one_or_higher,
    SUM(CASE WHEN sales_value IS NULL THEN 1 ELSE 0 END) > 0 AS has_missing_sales,
    SUM(CASE WHEN sales_value < 0 THEN 1 ELSE 0 END) > 0 AS has_negative_sales
FROM l2.l2_sales_long
GROUP BY product_key;

CREATE OR REPLACE TABLE l2.data_quality_kpis AS
SELECT
    COUNT(DISTINCT product_key) AS total_products,
    SUM(coeff_one_or_higher) AS tot_coeff_one_or_higher,
    SUM(has_missing_sales) AS tot_missing_sales,
    SUM(has_negative_sales) AS tot_negative_sales,
    ROUND(100.0 * SUM(coeff_one_or_higher)/COUNT(*), 2) AS pct_high_variability
FROM l2.sales_profile;