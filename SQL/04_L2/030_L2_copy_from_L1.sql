ATTACH 'SQL/L1_forecast_accuracy.duckdb' AS l1 (READ_ONLY);
ATTACH 'SQL/L2_forecast_accuracy.duckdb' AS l2;
--------------------------------------------------
-- PRODUCT DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE l2.dim_product AS
SELECT *
FROM l1.dim_product;
--------------------------------------------------
-- STORE DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE l2.dim_store AS
SELECT *
FROM l1.dim_store;
--------------------------------------------------
--------------------------------------------------
-- SELL PRICES DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE l2.dim_sell_prices AS
SELECT *
FROM l1.dim_sell_prices;
--------------------------------------------------
--------------------------------------------------
-- calendar DIMENSION
--------------------------------------------------
CREATE OR REPLACE TABLE l2.dim_calendar AS
SELECT *
FROM l1.dim_calendar;
--------------------------------------------------
--------------------------------------------------
-- sales
--------------------------------------------------
CREATE OR REPLACE TABLE l2.l2_sales AS
SELECT *
FROM l1.l1_sales;
--------------------------------------------------

DETACH DATABASE IF EXISTS l0;
DETACH DATABASE IF EXISTS l1;
DETACH DATABASE IF EXISTS l2;