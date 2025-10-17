CREATE OR REPLACE MACRO sales_join() AS TABLE (

SELECT *
FROM l2_sales_long AS f
LEFT JOIN dim_calendar AS c USING (date_key)
LEFT JOIN dim_store AS st USING (store_key)
LEFT JOIN dim_product AS p USING (product_key)
);