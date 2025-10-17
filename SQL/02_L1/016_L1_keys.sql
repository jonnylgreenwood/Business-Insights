-- ==========================================================
-- Add surrogate keys to existing dimensions & propagate them
-- ==========================================================

ATTACH 'SQL/L1_forecast_accuracy.duckdb' AS l1;
USE l1;

------------------------------------------------------------
-- 1️⃣ Add surrogate keys to dimension tables
------------------------------------------------------------

-- Product dimension
CREATE OR REPLACE TABLE dim_product AS
SELECT
    ROW_NUMBER() OVER () AS product_key,
    *
FROM dim_product;

-- Store dimension
CREATE OR REPLACE TABLE dim_store AS
SELECT
    ROW_NUMBER() OVER () AS store_key,
    *
FROM dim_store;

-- Calendar dimension
CREATE OR REPLACE TABLE dim_calendar AS
SELECT
    ROW_NUMBER() OVER (ORDER BY date) AS date_key,
    *
FROM dim_calendar;

