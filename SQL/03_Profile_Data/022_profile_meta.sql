COPY (
    SELECT *
    FROM duckdb_columns
) TO 'SQL/outputs/csv_for_markdown/profile_meta.csv' (FORMAT CSV, HEADER TRUE);
