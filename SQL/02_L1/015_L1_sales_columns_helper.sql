COPY (
  SELECT string_agg('"' || column_name || '"', ', ' ORDER BY column_index) AS cols
  FROM duckdb_columns
  WHERE table_name = 'l1_sales'
    AND column_name LIKE 'd_%'
)
TO 'SQL/outputs/015_L1_sales_columns_helper_col_list.txt' (FORMAT CSV, HEADER FALSE);
