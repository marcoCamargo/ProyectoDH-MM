
CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_TIME` AS

SELECT
  EXTRACT(DATE FROM payment_date) date_id,
  EXTRACT(DAY FROM payment_date) DAY,
  EXTRACT(MONTH FROM payment_date) MONTH,
  EXTRACT(YEAR FROM payment_date) YEAR,
  EXTRACT(QUARTER FROM payment_date) QUARTER,
  EXTRACT(DAYOFWEEK FROM payment_date) DAYOFWEEK,
  EXTRACT(WEEK FROM payment_date) WEEKOFYEAR
FROM (
  SELECT
    DISTINCT payment_date  
  FROM
  	 `{{ params.project_id }}.{{ params.staging_dataset }}.payment`)
 WHERE payment_date IS NOT NULL