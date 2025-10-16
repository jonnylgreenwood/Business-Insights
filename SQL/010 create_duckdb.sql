CREATE OR REPLACE TABLE sales_train_validation AS
SELECT * FROM read_csv_auto('data/m5-forecasting-accuracy/sales_train_validation.csv');

CREATE OR REPLACE TABLE sales_train_evaluation AS
SELECT * FROM read_csv_auto('data/m5-forecasting-accuracy/sales_train_evaluation.csv');

CREATE OR REPLACE TABLE sell_prices AS
SELECT * FROM read_csv_auto('data/m5-forecasting-accuracy/sell_prices.csv');

CREATE OR REPLACE TABLE calendar AS
SELECT * FROM read_csv_auto('data/m5-forecasting-accuracy/calendar.csv');

CREATE OR REPLACE TABLE sample_submission AS
SELECT * FROM read_csv_auto('data/m5-forecasting-accuracy/sample_submission.csv');