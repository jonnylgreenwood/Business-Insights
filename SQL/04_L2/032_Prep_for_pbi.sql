COPY (SELECT p.product_key, st.store_key, l2.sales, c.date, s.sell_price,
         s.sell_price * l2.sales AS revenue
FROM l2_sales_long l2
LEFT JOIN dim_calendar c
    ON l2.d = c.d
LEFT JOIN dim_sell_prices s
    ON l2.product_id = s.product_id
    AND l2.store_id = s.store_id
    AND c.wm_yr_wk = s.week_id
LEFT JOIN dim_product p
    ON l2.product_id = p.product_id
LEFT JOIN dim_store st
    ON l2.store_id = st.store_id
)
  TO 'SQL/outputs/parquet/fact_sales_enriched.parquet' (FORMAT PARQUET);


COPY (SELECT * FROM dim_calendar)
  TO 'SQL/outputs/parquet/dim_calendar.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM dim_event)
  TO 'SQL/outputs/parquet/dim_event.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM dim_product)
  TO 'SQL/outputs/parquet/dim_product.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM dim_snap)
  TO 'SQL/outputs/parquet/dim_snap.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM dim_sell_prices)
  TO 'SQL/outputs/parquet/dim_sell_prices.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM dim_store)
  TO 'SQL/outputs/parquet/dim_store.parquet' (FORMAT PARQUET);