ATTACH 'SQL/L2_forecast_accuracy.duckdb' AS l2;
USE l2;

INSERT INTO l2.dim_calendar
SELECT
    (SELECT MAX(date_key) FROM dim_calendar) + ROW_NUMBER() OVER () AS date_key,
    d AS date,
    (SELECT MAX(wm_yr_wk) FROM dim_calendar) + ROW_NUMBER() OVER () AS wm_yr_wk,
    strftime(d, '%A') AS weekday,
    EXTRACT(DAYOFWEEK FROM d) AS wday,
    EXTRACT(MONTH FROM d) AS month,
    EXTRACT(YEAR FROM d) AS year,
    CONCAT('d_', (SELECT MAX(CAST(SUBSTR(d, 3) AS BIGINT)) FROM dim_calendar)
                 + ROW_NUMBER() OVER ()) AS d,
    NULL AS event_name_1,
    NULL AS event_type_1,
    NULL AS event_name_2,
    NULL AS event_type_2,
    NULL AS snap_CA,
    NULL AS snap_TX,
    NULL AS snap_WI,
    TRUE AS isfuture
FROM (
    SELECT UNNEST(
        generate_series(
            (SELECT MAX(date) + INTERVAL 1 DAY FROM dim_calendar),
            (SELECT MAX(date) + INTERVAL 365 DAY FROM dim_calendar),
            INTERVAL 1 DAY
        )
    ) AS d
);