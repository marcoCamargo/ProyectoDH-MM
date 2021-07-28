
CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_CUSTOMER` AS

SELECT 
  DISTINCT customer_id,
  first_name,
  last_name,
  email
  FROM `{{ params.project_id }}.{{ params.staging_dataset }}.customer` 