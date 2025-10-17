DETACH DATABASE IF EXISTS l0;
DETACH DATABASE IF EXISTS l1;
DETACH DATABASE IF EXISTS l2;

ATTACH 'SQL/L1_forecast_accuracy.duckdb' AS l1 (READ_ONLY);

--------------------------------------------------
-- PRODUCT DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE dim_product AS
SELECT
    ROW_NUMBER() OVER () AS product_key,
    *
FROM l1.dim_product;

--------------------------------------------------
-- STORE DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE dim_store AS
SELECT
    ROW_NUMBER() OVER () AS store_key,
    *
FROM l1.dim_store;
--------------------------------------------------


--------------------------------------------------
-- SELL PRICES DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE dim_sell_prices AS
SELECT *
FROM l1.dim_sell_prices;
--------------------------------------------------

--------------------------------------------------
-- calendar DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE dim_calendar AS
SELECT *
FROM l1.dim_calendar;
--------------------------------------------------

--------------------------------------------------
-- sales_long
--------------------------------------------------
CREATE OR REPLACE TABLE l2_sales_long AS
SELECT *
FROM l1.l1_sales_long;
--------------------------------------------------