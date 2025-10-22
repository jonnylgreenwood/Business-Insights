ATTACH 'SQL/L2_forecast_accuracy.duckdb' AS l2;
USE l2;

CREATE OR REPLACE TABLE l2.dim_calendar_periods AS
WITH sales_dates AS (
    SELECT 
        s.date_key,
        c.date
    FROM l2.l2_sales_long AS s
    JOIN l2.dim_calendar AS c
        ON s.date_key = c.date_key
    WHERE s.sales > 0
),
last_actual AS (
    SELECT MAX(date) AS last_actual_date FROM sales_dates
),
period_bounds AS (
    SELECT 'Current Year' AS period,
           MAKE_DATE(EXTRACT(YEAR FROM l.last_actual_date)::INT, 1, 1) AS start_date,
           l.last_actual_date AS end_date
    FROM last_actual l
    UNION ALL
    SELECT 'Prior Year',
           MAKE_DATE((EXTRACT(YEAR FROM l.last_actual_date)-1)::INT, 1, 1),
           l.last_actual_date - INTERVAL '1 year'
    FROM last_actual l
    UNION ALL
    SELECT 'Rolling 3M',  l.last_actual_date - INTERVAL '3 months',  l.last_actual_date FROM last_actual l
    UNION ALL
    SELECT 'Rolling 6M',  l.last_actual_date - INTERVAL '6 months',  l.last_actual_date FROM last_actual l
    UNION ALL
    SELECT 'Rolling 12M', l.last_actual_date - INTERVAL '12 months', l.last_actual_date FROM last_actual l
    UNION ALL
    SELECT 'Current Month',
           MAKE_DATE(EXTRACT(YEAR FROM l.last_actual_date)::INT, EXTRACT(MONTH FROM l.last_actual_date)::INT, 1),
           l.last_actual_date
    FROM last_actual l
    UNION ALL
    SELECT 'Prior Month',
           DATE_TRUNC('month', l.last_actual_date - INTERVAL '1 month'),
           (DATE_TRUNC('month', l.last_actual_date) - INTERVAL '1 day')
    FROM last_actual l
)
SELECT 
    p.period,
    gs.date AS date
FROM period_bounds AS p,
     generate_series(p.start_date, p.end_date, INTERVAL '1 day') AS gs(date)
ORDER BY p.period, gs.date;
