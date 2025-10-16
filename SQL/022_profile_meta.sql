COPY (
    SELECT *
    FROM duckdb_columns
) TO 'SQL/schema_columns.md' (DELIMITER '|', HEADER TRUE);
