ATTACH 'SQL/L2_business_insights.duckdb' AS l2;
USE l2;


CREATE OR REPLACE TABLE l2.l2_sales_long_extended AS
SELECT 
        c.date_key,
        p.product_key,
        st.store_key,
        COALESCE(l.sales, 0) AS sales,
        sp.sell_price,
        COALESCE(sp.sell_price * l.sales, 0)::FLOAT AS sales_value
    FROM dim_calendar c
    CROSS JOIN dim_product p
    CROSS JOIN dim_store st
    LEFT JOIN l2_sales_long l 
        ON l.date_key = c.date_key
       AND l.product_key = p.product_key
       AND l.store_key = st.store_key
    LEFT JOIN dim_sell_prices sp
        ON c.wm_yr_wk = sp.wm_yr_wk
        AND l.product_key = sp.product_key
        AND l.store_key = sp.store_key;