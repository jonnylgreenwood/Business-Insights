------------------------------------------------------------
-- Unpivot L1 sales_base into a long (fact) format
------------------------------------------------------------
CREATE OR REPLACE TABLE l1_sales_long AS
SELECT
    product_id,
    store_id,
    day_col AS d,
    NULLIF(sales_value, 0) AS sales  -- optional: replace 0s with NULL here
FROM l1_sales
UNPIVOT (sales_value FOR day_col IN (d_1:d_1913))
WHERE sales IS NOT NULL;  -- optional: filter out rows where sales is NULL
DESCRIBE l1_sales_long;