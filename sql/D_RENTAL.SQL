CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_RENTAL` AS

select distinct rental_id,
       title,
       release_year,
       rental_duration,
       rental_rate,
       tb3.length,
       replacement_cost,
       rating,
       tb4.name
  from `{{ params.project_id }}.{{ params.staging_dataset }}.rental` tb1
  inner join `{{ params.project_id }}.{{ params.staging_dataset }}.inventory` tb2 on tb1.inventory_id = tb2.inventory_id
  inner join `{{ params.project_id }}.{{ params.staging_dataset }}.film` tb3 on tb2.film_id = tb3.film_id 
  inner join `{{ params.project_id }}.{{ params.staging_dataset }}.language` tb4 on tb4.language_id = tb3.language_id
