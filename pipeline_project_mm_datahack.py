# Import packages
#from airflow import DAG
import os
from airflow.models import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator


# Define default arguments
default_args = {
    'owner': 'Melvy Perez - Marco Camargo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

# Define dag variables
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/marcoCamargo/Downloads/key.json'
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "celestial-gecko-320402")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET_NAME", "ventas_mm")
project_id = 'celestial-gecko-320402'
staging_dataset = 'PAGOS_DWH_STAGING'
dwh_dataset = 'PAGOS_DWH'
gs_bucket = 'ventas_mm'

# Define dag
with DAG('proyecto_datahack_mm',
    start_date=datetime.now(),
    schedule_interval='@once',
    concurrency=5,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    start_pipeline = DummyOperator(task_id="inicio_pipeline")

    # Load data from GCS to BQ
    load_actors = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_actors',
        bucket = gs_bucket,
        source_objects = ['actor.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.actors',
        schema_object = 'actors.json',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
        skip_leading_rows = 1,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    load_payment = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_payment',
        bucket = gs_bucket,
        source_objects = ['payment.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.payment',
        schema_object = 'payment.json',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
        skip_leading_rows = 1,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    load_address = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_address',
        bucket = gs_bucket,
        source_objects = ['address.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.address',
        schema_object = 'address.json',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
        skip_leading_rows = 1,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    load_category = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_category',
        bucket = gs_bucket,
        source_objects = ['category.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.category',
        schema_object = 'category.json',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
        skip_leading_rows = 1,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    load_city = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_city',
        bucket = gs_bucket,
        source_objects = ['city.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.city',
        schema_object = 'city.json',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
        skip_leading_rows = 1,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    load_country = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_country',
        bucket = gs_bucket,
        source_objects = ['country.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.country',
        schema_object = 'country.json',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
        skip_leading_rows = 1,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    load_customer = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_customer',
        bucket = gs_bucket,
        source_objects = ['customer.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.customer',
        schema_object = 'customer.json',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
        skip_leading_rows = 1,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )


    load_film = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_film',
        bucket = gs_bucket,
        source_objects = ['film.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.film',
        schema_object = 'film.json',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
        skip_leading_rows = 1,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    load_film_actor = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_film_actor',
        bucket = gs_bucket,
        source_objects = ['film_actor.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.film_actor',
        schema_object = 'film_actor.json',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
        skip_leading_rows = 1,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    load_inventory = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_inventory',
        bucket = gs_bucket,
        source_objects = ['inventory.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.inventory',
        schema_object = 'inventory.json',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
        skip_leading_rows = 1,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    load_language = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_language',
        bucket = gs_bucket,
        source_objects = ['language.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.language',
        schema_object = 'language.json',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
        skip_leading_rows = 1,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    load_rental = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_rental',
        bucket = gs_bucket,
        source_objects = ['rental.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.rental',
        schema_object = 'rental.json',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
        skip_leading_rows = 1,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    load_staff = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_staff',
        bucket = gs_bucket,
        source_objects = ['staff.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.staff',
        schema_object = 'staff.json',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
        skip_leading_rows = 1,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    load_store = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_store',
        bucket = gs_bucket,
        source_objects = ['store.csv'],
        destination_project_dataset_table = f'{project_id}:{staging_dataset}.store',
        schema_object = 'store.json',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
        skip_leading_rows = 1,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )


    # Check loaded data not null
    check_actors = BigQueryCheckOperator(
        task_id = 'check_actors',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.actors`'

    )

    check_payment = BigQueryCheckOperator(
        task_id = 'check_payment',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.payment`'
    )

    check_address = BigQueryCheckOperator(
        task_id = 'check_address',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.address`'
    )

    check_category = BigQueryCheckOperator(
        task_id = 'check_category',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.category`'
    )

    check_city = BigQueryCheckOperator(
        task_id = 'check_city',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.city`'
    )

    check_country = BigQueryCheckOperator(
        task_id = 'check_country',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.country`'
    )

    check_film = BigQueryCheckOperator(
        task_id = 'check_film',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.film`'
    )


    check_film_actor = BigQueryCheckOperator(
        task_id = 'check_film_actor',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.film_actor`'
    )

    check_inventory = BigQueryCheckOperator(
        task_id = 'check_inventory',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.inventory`'
    )

    check_language = BigQueryCheckOperator(
        task_id = 'check_language',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.language`'
    )

    check_rental = BigQueryCheckOperator(
        task_id = 'check_rental',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.rental`'
    )

    check_staff = BigQueryCheckOperator(
        task_id = 'check_staff',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.staff`'
    )

    check_store= BigQueryCheckOperator(
        task_id = 'check_store',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.store`'
    )

    check_customer= BigQueryCheckOperator(
        task_id = 'check_customer',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.customer`'
    )

    loaded_data_to_staging = DummyOperator(task_id="loaded_data_to_staging")


    # Create dimensions data
    create_d_time = BigQueryOperator(
        task_id = 'create_d_time',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        sql = './sql/D_TIME.sql'
    )

    create_d_customer = BigQueryOperator(
        task_id = 'create_d_customer',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        sql = './sql/D_CUSTOMER.sql'
    )

    create_d_rental = BigQueryOperator(
        task_id = 'create_d_rental',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        sql = './sql/D_RENTAL.sql'
    )

    # creamos y cargamos la FACT
    create_fact_pagos = BigQueryOperator(
        task_id = 'create_fact_pagos',
        use_legacy_sql = False,
        params = {
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        sql = './sql/FACT_PAGOS.sql'
    )

    check_FACT= BigQueryCheckOperator(
        task_id = 'check_FACT',
        use_legacy_sql=False,
        sql = f'SELECT count(*) FROM `{project_id}.{dwh_dataset}.FACT_PAGOS`'
    )

    fin_pipeline = DummyOperator(task_id="fin_pipeline")

    start_pipeline >> [load_actors, load_payment, load_address, load_category, load_city, load_country, load_customer, load_film, load_film_actor, load_inventory, load_language, load_rental, load_staff, load_store]

    # check data on GCP
    load_actors  >> check_actors 
    load_payment >> check_payment
    load_address >> check_address
    load_category >> check_category 
    load_city >>  check_city
    load_country >> check_country
    load_customer >> check_customer
    load_film >> check_film
    load_film_actor >> check_film_actor
    load_inventory >> check_inventory 
    load_language >> check_language 
    load_rental >> check_rental
    load_staff >> check_staff
    load_store >> check_store

    [check_actors, check_payment, check_address,check_category,check_city,check_country, check_customer, check_film, check_film_actor, check_inventory, check_language, check_rental, check_staff, check_store] >> loaded_data_to_staging

    loaded_data_to_staging >> [create_d_time, create_d_customer, create_d_rental]

    [create_d_time, create_d_customer, create_d_rental] >> create_fact_pagos

    create_fact_pagos >> check_FACT >> fin_pipeline
