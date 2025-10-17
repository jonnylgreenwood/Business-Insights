-- Cleanly reattach all databases
DETACH DATABASE IF EXISTS l0;
DETACH DATABASE IF EXISTS l1;
DETACH DATABASE IF EXISTS l2;

ATTACH 'SQL/L1_forecast_accuracy.duckdb' AS l1 (READ_ONLY);

-- Combine column metadata across all attached DBs
CREATE OR REPLACE TABLE meta_all_columns AS
SELECT
  database_name,
  schema_name,
  table_name,
  column_name,
  data_type,
  is_nullable,
  column_index
FROM duckdb_columns
WHERE database_name IN ('l1')
ORDER BY database_name, table_name, column_index;

COPY (
  SELECT *
  FROM meta_all_columns
) TO 'SQL/outputs/csv_for_markdown/l1_db_columns.csv' (FORMAT CSV, HEADER TRUE);

