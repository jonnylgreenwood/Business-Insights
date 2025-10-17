duckdb SQL/L0_forecast_accuracy.duckdb
duckdb SQL/L1_forecast_accuracy.duckdb
duckdb SQL/L2_forecast_accuracy.duckdb

.read SQL/

011_select_all.sql
012_L1_hierarchies.sql
013_L1_sales.sql
014_L1_sales_long.sql
SQL/030_L2_copy_from_L1.sql
SQL/902_get_l2_metadata.sql

TO 'SQL/outputs/csv_for_markdown/xx.csv' (FORMAT CSV, HEADER TRUE); //to output to .csv
SQL/02_L1/014_L1_sales_long.sql
duckdb SQL/L2_forecast_accuracy.duckdb -f SQL/902_get_l1_metadata.sql
duckdb SQL/L2_forecast_accuracy.duckdb -f SQL/030_L2_copy_from_L1.sql
duckdb SQL/L2_forecast_accuracy.duckdb -f SQL/99_Utility/902_get_l2_metadata.sql
duckdb SQL/L2_forecast_accuracy.duckdb -f SQL/04_L2/030_L2_copy_from_L1.sql
duckdb SQL/L2_forecast_accuracy.duckdb -f SQL/04_L2/032_Prep_for_pbi.sql
duckdb SQL/L2_forecast_accuracy.duckdb -f SQL/04_L2/033_check_parquets.sql
SQL/04_L2/033_check_parquets.sql
SQL/outputs/parquet
SQL/04_L2/00_Create_events
SQL/04_L2/033_check_parquets

duckdb SQL/L2_forecast_accuracy.duckdb -f

SQL/04_L2/00_Create_events.sql
duckdb SQL/L2_forecast_accuracy.duckdb -f SQL/04_L2/00_Create_events.sql

duckdb -f SQL/05_data_quality_checks/01_Check_sales

SQL/02_L1/014_L1_sales_long.sql

duckdb SQL/L1_forecast_accuracy.duckdb -f SQL/02_L1/014_L1_sales_long.sql