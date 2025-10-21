ATTACH 'SQL/L2_forecast_accuracy.duckdb' AS l2;
USE l2;


CREATE OR REPLACE TABLE l2.l2_sales_long_extended AS
SELECT 
        c.date_key,
        p.product_key,
        st.store_key,
        COALESCE(l.sales, 0) AS sales
    FROM dim_calendar c
    CROSS JOIN dim_product p
    CROSS JOIN dim_store st
    LEFT JOIN l2_sales_long l 
        ON l.date_key = c.date_key
       AND l.product_key = p.product_key
       AND l.store_key = st.store_key