ATTACH 'SQL/L2_forecast_accuracy.duckdb' AS l2;
USE l2;

COPY (SELECT * FROM l2.l2_sales_long)
  TO 'SQL/outputs/parquet/l2_sales_long.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM l2.dim_calendar)
  TO 'SQL/outputs/parquet/dim_calendar.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM l2.dim_product)
  TO 'SQL/outputs/parquet/dim_product.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM l2.dim_sell_prices)
  TO 'SQL/outputs/parquet/dim_sell_prices.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM l2.dim_store)
  TO 'SQL/outputs/parquet/dim_store.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM l2.sales_profile)
  TO 'SQL/outputs/parquet/sales_profile.parquet' (FORMAT PARQUET);
