SELECT
    COUNT(*) AS total_rows,
    AVG(sales) AS avg_sales,
    MIN(sales) AS min_sales,
    MAX(sales) AS max_sales,
    STDDEV(sales) AS std_sales,
    APPROX_QUANTILE(sales, 0.5) AS median_sales
FROM l1_sales_long;