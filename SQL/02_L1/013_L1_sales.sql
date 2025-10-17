DETACH DATABASE IF EXISTS l0;
ATTACH 'SQL/L0_forecast_accuracy.duckdb' AS l0 (READ_ONLY);

CREATE OR REPLACE TABLE l1_sales AS
SELECT *
FROM l0.sales_train_validation;

ALTER TABLE sales_base DROP COLUMN id;
ALTER TABLE sales_base DROP COLUMN dept_id;
ALTER TABLE sales_base DROP COLUMN cat_id;
ALTER TABLE sales_base DROP COLUMN state_id;
ALTER TABLE sales_base RENAME COLUMN item_id TO product_id;

DESCRIBE sales_base;