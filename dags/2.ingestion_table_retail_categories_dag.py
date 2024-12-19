from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery


from ingestion_table_retail_categories import (
    setup_bigquery_credentials,
    get_most_recent_file,
    verify_and_create_tables,
    load_data_to_raw,
    merge_raw_to_trusted,
)

# Configurações do projeto
PROJECT_ID = "bankmarketingdatapipeline"
DATASET_NAME = "db_retail"
RAW_TABLE_NAME = "raw_categories"
TRUSTED_TABLE_NAME = "trusted_categories"
BUCKET_NAME = "ingestion-raw-data-retail"
PREFIX = "categories/"



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
}

with DAG(
    '1-load_to_bigquery_dag',
    default_args=default_args,
    description='Pipeline para carga no BigQuery',
    schedule_interval='0 4 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    setup_credentials = PythonOperator(
        task_id='setup_bigquery_credentials',
        python_callable=setup_bigquery_credentials,
    )

    get_recent_file = PythonOperator(
        task_id='get_most_recent_file',
        python_callable=get_most_recent_file,
    )

    verify_tables = PythonOperator(
        task_id='verify_and_create_tables',
        python_callable=verify_and_create_tables,
    )

    load_raw = PythonOperator(
        task_id='load_data_to_raw',
        python_callable=load_data_to_raw,
    )

    merge_data = PythonOperator(
        task_id='merge_raw_to_trusted',
        python_callable=merge_raw_to_trusted,
    )

    setup_credentials >> get_recent_file >> verify_tables >> load_raw >> merge_data

