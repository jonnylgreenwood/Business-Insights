ATTACH 'SQL/L0_forecast_accuracy.duckdb' AS l0 (READ_ONLY);

--------------------------------------------------
-- PRODUCT DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE dim_product AS
SELECT DISTINCT
    cat_id AS category_id,
    dept_id AS department_id,
    item_id AS product_id
FROM l0.sales_train_validation;
--------------------------------------------------

--------------------------------------------------
-- STORE DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE dim_store AS
SELECT DISTINCT
    store_id AS store_id,
    state_id AS state_id,
FROM l0.sales_train_validation;
--------------------------------------------------


--------------------------------------------------
-- SELL PRICES DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE dim_sell_prices AS
SELECT DISTINCT
    store_id AS store_id,
    item_id AS product_id,
    wm_yr_wk AS week_id,
    sell_price AS sell_price
FROM l0.sell_prices;
--------------------------------------------------