CREATE OR REPLACE TABLE dim_snap AS
SELECT date, 'CA' AS state, snap_CA AS snap_flag
FROM dim_calendar
UNION ALL
SELECT date, 'TX' AS state, snap_TX AS snap_flag
FROM dim_calendar
UNION ALL
SELECT date, 'WI' AS state, snap_WI AS snap_flag
FROM dim_calendar;
