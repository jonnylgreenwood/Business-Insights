DETACH DATABASE IF EXISTS l0;
ATTACH 'SQL/L0_forecast_accuracy.duckdb' AS l0 (READ_ONLY);

------------------------------------------------------------
-- 1. Copy the sales table from L0 to L1
------------------------------------------------------------
CREATE OR REPLACE TABLE l1_sales AS
SELECT
    store_id,
    item_id AS product_id,
    columns('d_*')
FROM l0.sales_train_evaluation;