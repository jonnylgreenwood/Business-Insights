ATTACH 'SQL/L2_forecast_accuracy.duckdb' AS l2;
USE l2;

-- SELECT 
--     SUM('is_month_end'),
--     SUM('is_month_start'),
--     SUM('is_weekend'),
--     SUM('is_black_friday'),
--     SUM('is_christmas_period'),
--     SUM('is_valentines'),
--     SUM('is_easter'),
--     SUM('is_superbowl'),
--     SUM('is_presidents_day'),
--     SUM('is_cinco_de_mayo'),
--     SUM('is_orthodox_easter'),
--     SUM('is_independence_day'),
--     SUM('is_new_year')
-- FROM l2.calendar_events;

CREATE OR REPLACE TABLE l2.event_window_lifts AS
SELECT 'month_end'        AS holiday, * FROM l2.event_window_lift('is_month_end')
UNION ALL
SELECT 'month_start'      AS holiday, * FROM l2.event_window_lift('is_month_start')
UNION ALL
SELECT 'weekend'          AS holiday, * FROM l2.event_window_lift('is_weekend')
UNION ALL
SELECT 'black_friday'     AS holiday, * FROM l2.event_window_lift('is_black_friday')
UNION ALL
SELECT 'christmas_period' AS holiday, * FROM l2.event_window_lift('is_christmas_period')
UNION ALL
SELECT 'valentines'       AS holiday, * FROM l2.event_window_lift('is_valentines')
UNION ALL
SELECT 'easter'           AS holiday, * FROM l2.event_window_lift('is_easter')
UNION ALL
SELECT 'superbowl'        AS holiday, * FROM l2.event_window_lift('is_superbowl')
UNION ALL
SELECT 'presidents_day'   AS holiday, * FROM l2.event_window_lift('is_presidents_day')
UNION ALL
SELECT 'cinco_de_mayo'    AS holiday, * FROM l2.event_window_lift('is_cinco_de_mayo')
UNION ALL
SELECT 'orthodox_easter'  AS holiday, * FROM l2.event_window_lift('is_orthodox_easter')
UNION ALL
SELECT 'independence_day' AS holiday, * FROM l2.event_window_lift('is_independence_day')
UNION ALL
SELECT 'new_year'         AS holiday, * FROM l2.event_window_lift('is_new_year');

CREATE OR REPLACE TABLE l2.event_windows_pearson_correlations AS
SELECT
  corr(sales_value, is_weekend)       AS r_weekend,
  corr(sales_value, is_month_end)     AS r_month_end,
  corr(sales_value, is_month_start)   AS r_month_start
FROM sales_join()
JOIN calendar_events USING (date_key);