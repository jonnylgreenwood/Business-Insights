DETACH DATABASE IF EXISTS l0;
ATTACH 'SQL/L0_forecast_accuracy.duckdb' AS l0 (READ_ONLY);

CREATE OR REPLACE TABLE l1_sales AS
SELECT st.store_key,
       p.product_key,
       s.*
FROM l0.sales_train_validation s
LEFT JOIN dim_store st
    ON s.store_id = st.store_id
LEFT JOIN dim_product p
    ON s.product_id = p.product_id;

ALTER TABLE l1_sales DROP COLUMN id;
ALTER TABLE l1_sales DROP COLUMN dept_id;
ALTER TABLE l1_sales DROP COLUMN cat_id;
ALTER TABLE l1_sales DROP COLUMN state_id;
ALTER TABLE l1_sales RENAME COLUMN item_id TO product_id;

DESCRIBE l1_sales;