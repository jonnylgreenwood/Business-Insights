ATTACH 'SQL/L2_business_insights.duckdb' AS l2;
USE l2;

CREATE OR REPLACE TABLE l2.calendar_events AS
SELECT 
    date_key,
    CAST(date AS DATE) AS date,
    month,
    wday AS weekday_index,
    
    -- Named events
    CASE WHEN event_name_1 = 'SuperBowl' THEN 1 ELSE 0 END AS is_superbowl,
    CASE WHEN event_name_1 = 'ValentinesDay' THEN 1 ELSE 0 END AS is_valentines,
    CASE WHEN event_name_1 = 'PresidentsDay' THEN 1 ELSE 0 END AS is_presidents_day,
    CASE WHEN event_name_2 = 'Easter' THEN 1 ELSE 0 END AS is_easter,
    CASE WHEN event_name_2 = 'Cinco De Mayo' THEN 1 ELSE 0 END AS is_cinco_de_mayo,
    CASE WHEN event_name_2 = 'OrthodoxEaster' THEN 1 ELSE 0 END AS is_orthodox_easter,

    -- Date-based events
    CASE 
        WHEN month = 7 AND CAST(strftime('%d', CAST(date AS DATE)) AS INTEGER) = 4 
        THEN 1 ELSE 0 
    END AS is_independence_day,

    CASE 
        WHEN month = 1 AND CAST(strftime('%d', CAST(date AS DATE)) AS INTEGER) = 1 
        THEN 1 ELSE 0 
    END AS is_new_year,

    CASE 
        WHEN month = 12 
             AND CAST(strftime('%d', CAST(date AS DATE)) AS INTEGER) BETWEEN 20 AND 31 
        THEN 1 ELSE 0 
    END AS is_christmas,

    CASE 
        -- Friday (weekday = 5) in November
        WHEN month = 11 AND strftime('%w', CAST(date AS DATE)) = '5' 
        THEN 1 ELSE 0 
    END AS is_black_friday,

    CASE 
        WHEN CAST(strftime('%d', CAST(date AS DATE)) AS INTEGER) >= 26 
        THEN 1 ELSE 0 
    END AS is_month_end,

    CASE 
        WHEN CAST(strftime('%d', CAST(date AS DATE)) AS INTEGER) <= 4 
        THEN 1 ELSE 0 
    END AS is_month_start,

    CASE 
        WHEN strftime('%w', CAST(date AS DATE)) IN ('0','6') 
        THEN 1 ELSE 0 
    END AS is_weekend

FROM l2.dim_calendar;

