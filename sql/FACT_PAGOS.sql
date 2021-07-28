CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.FACT_PAGOS` AS

select customer_id,
       rental_id,
       extract(date from payment_date) date_id,
       sum(amount) amount
  from `{{ params.project_id }}.{{ params.staging_dataset }}.payment`
group by customer_id,
         rental_id,
         extract(date from payment_date)