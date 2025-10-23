ATTACH 'SQL/L0_business_insights.duckdb' AS l0 (READ_ONLY);
ATTACH 'SQL/L1_business_insights.duckdb' AS l1;

--------------------------------------------------
-- PRODUCT DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE l1.dim_product AS
SELECT DISTINCT
    cat_id AS cat_id,
    dept_id AS dept_id,
    item_id AS item_id
FROM l0.sales_train_validation;
--------------------------------------------------

--------------------------------------------------
-- STORE DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE l1.dim_store AS
SELECT DISTINCT
    store_id AS store_id,
    state_id AS state_id,
FROM l0.sales_train_validation;
--------------------------------------------------


--------------------------------------------------
-- SELL PRICES DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE l1.dim_sell_prices AS
SELECT DISTINCT
    store_id AS store_id,
    item_id AS item_id,
    wm_yr_wk AS wm_yr_wk,
    sell_price AS sell_price
FROM l0.sell_prices;
--------------------------------------------------

--------------------------------------------------
-- calendar DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE l1.dim_calendar AS
SELECT *
FROM l0.calendar;
--------------------------------------------------

--------------------------------------------------
-- sales DIMENSION
--------------------------------------------------

CREATE OR REPLACE TABLE l1.l1_sales AS
SELECT *
FROM l0.sales_train_evaluation;

DETACH DATABASE IF EXISTS l0;
DETACH DATABASE IF EXISTS l1;
DETACH DATABASE IF EXISTS l2;