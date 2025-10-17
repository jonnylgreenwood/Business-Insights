ATTACH 'SQL/L2_forecast_accuracy.duckdb' AS l2;
USE l2;

SELECT store_id, COUNT(DISTINCT store_key) AS stores, COUNT(DISTINCT product_key) AS products, COUNT(store_id) AS records
FROM sales_join() AS f
GROUP BY store_id;

SELECT state_id, COUNT(DISTINCT store_key) AS stores, COUNT(DISTINCT product_key) AS products, COUNT(store_id) AS records
FROM sales_join() AS f
GROUP BY state_id;