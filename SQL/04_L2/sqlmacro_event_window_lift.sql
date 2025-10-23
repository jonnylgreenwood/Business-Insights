CREATE OR REPLACE MACRO event_window_lift(flag_name) AS TABLE (
    WITH s AS (
        SELECT
            s.sales_value,
            s.date,
            s.product_key,
            CASE 
                WHEN flag_name = 'is_month_end'        THEN c.is_month_end
                WHEN flag_name = 'is_month_start'      THEN c.is_month_start
                WHEN flag_name = 'is_weekend'          THEN c.is_weekend
                WHEN flag_name = 'is_black_friday'     THEN c.is_black_friday
                WHEN flag_name = 'is_christmas'        THEN c.is_christmas
                WHEN flag_name = 'is_valentines'       THEN c.is_valentines
                WHEN flag_name = 'is_easter'           THEN c.is_easter
                WHEN flag_name = 'is_superbowl'        THEN c.is_superbowl
                WHEN flag_name = 'is_presidents_day'   THEN c.is_presidents_day
                WHEN flag_name = 'is_cinco_de_mayo'    THEN c.is_cinco_de_mayo
                WHEN flag_name = 'is_orthodox_easter'  THEN c.is_orthodox_easter
                WHEN flag_name = 'is_independence_day' THEN c.is_independence_day
                WHEN flag_name = 'is_new_year'         THEN c.is_new_year
                ELSE 0
            END AS event_flag
        FROM sales_join() AS s
        JOIN calendar_events c USING (date_key)
    ),
    events AS (
        SELECT DISTINCT CAST(date AS DATE) AS event_date
        FROM s
        WHERE event_flag = 1
    ),
    baseline AS (
        SELECT AVG(sales_value) AS baseline_sales
        FROM s
        WHERE event_flag = 0
    ),

    -- 7 days before
    before7 AS (
        SELECT '7_days_before' AS event_window, AVG(s.sales_value) AS avg_sales
        FROM s
        JOIN events e
          ON s.date BETWEEN (e.event_date - INTERVAL 7 DAY) AND (e.event_date - INTERVAL 1 DAY)
    ),

    -- 3 days before
    before3 AS (
        SELECT '3_days_before' AS event_window, AVG(s.sales_value) AS avg_sales
        FROM s
        JOIN events e
          ON s.date BETWEEN (e.event_date - INTERVAL 3 DAY) AND (e.event_date - INTERVAL 1 DAY)
    ),

    -- 3 days before and after
    before_after3 AS (
        SELECT '3_days_before_and_after' AS event_window, AVG(s.sales_value) AS avg_sales
        FROM s
        JOIN events e
          ON s.date BETWEEN (e.event_date - INTERVAL 3 DAY) AND (e.event_date + INTERVAL 3 DAY)
    ),

    -- Day of event
    day0 AS (
        SELECT 'day_of_event' AS event_window, AVG(s.sales_value) AS avg_sales
        FROM s
        JOIN events e
          ON s.date = e.event_date
    ),

    -- 7 days after
    after7 AS (
        SELECT '7_days_after' AS event_window, AVG(s.sales_value) AS avg_sales
        FROM s
        JOIN events e
          ON s.date BETWEEN (e.event_date + INTERVAL 1 DAY) AND (e.event_date + INTERVAL 7 DAY)
    ),

    -- Outside window (no join)
    outside AS (
        SELECT 'outside_window' AS event_window, AVG(s.sales_value) AS avg_sales
        FROM s
        WHERE event_flag = 0
    )

    -- Combine them all
    SELECT
        u.event_window,
        u.avg_sales,
        ROUND(100.0 * (u.avg_sales - b.baseline_sales) / b.baseline_sales, 2) AS pct_lift
    FROM (
        SELECT * FROM before7
        UNION ALL
        SELECT * FROM before3
        UNION ALL
        SELECT * FROM before_after3
        UNION ALL
        SELECT * FROM day0
        UNION ALL
        SELECT * FROM after7
        UNION ALL
        SELECT * FROM outside
    ) u
    CROSS JOIN baseline b
    ORDER BY 
        CASE u.event_window
            WHEN '7_days_before' THEN 1
            WHEN '3_days_before' THEN 2
            WHEN '3_days_before_and_after' THEN 3
            WHEN 'day_of_event' THEN 4
            WHEN '7_days_after' THEN 5
            ELSE 6
        END
);
