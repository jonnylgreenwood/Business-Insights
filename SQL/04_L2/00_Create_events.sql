CREATE OR REPLACE TABLE dim_event AS
SELECT
    date,
    event_name_1 AS event_name,
    event_type_1 AS event_type
FROM dim_calendar
WHERE event_name_1 IS NOT NULL

UNION ALL

SELECT
    date,
    event_name_2 AS event_name,
    event_type_2 AS event_type
FROM dim_calendar
WHERE event_name_2 IS NOT NULL;