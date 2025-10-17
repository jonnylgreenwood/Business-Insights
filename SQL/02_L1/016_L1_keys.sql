ATTACH 'SQL/L1_forecast_accuracy.duckdb' AS l1;
------------------------------------------------------------
-- 1️⃣ Add surrogate keys to dimension tables
------------------------------------------------------------

-- Product dimension
CREATE OR REPLACE TABLE l1.dim_product AS
SELECT
    ROW_NUMBER() OVER () AS product_key,
    *
FROM l1.dim_product;

-- Store dimension
CREATE OR REPLACE TABLE l1.dim_store AS
SELECT
    ROW_NUMBER() OVER () AS store_key,
    *
FROM l1.dim_store;

-- Calendar dimension
CREATE OR REPLACE TABLE l1.dim_calendar AS
SELECT
    ROW_NUMBER() OVER (ORDER BY date) AS date_key,
    *
FROM l1.dim_calendar;


-- dim_sell_prices
CREATE OR REPLACE TABLE l1.dim_sell_prices AS
SELECT
    ROW_NUMBER() OVER () AS sell_price_key,
    st.store_key,
    p.product_key,
    s.wm_yr_wk,
    s.sell_price
FROM l1.dim_sell_prices s
LEFT JOIN l1.dim_store st
    ON s.store_id = st.store_id
LEFT JOIN l1.dim_product p
    ON s.item_id = p.item_id;


CREATE OR REPLACE TABLE l1.l1_sales AS
SELECT 
    ROW_NUMBER() OVER () AS sales_key,
    st.store_key,
    p.product_key,
    s.*
FROM l1.l1_sales s
LEFT JOIN l1.dim_store st
    ON s.store_id = st.store_id
LEFT JOIN l1.dim_product p
    ON s.item_id = p.item_id;

DETACH DATABASE IF EXISTS l0;
DETACH DATABASE IF EXISTS l1;
DETACH DATABASE IF EXISTS l2;