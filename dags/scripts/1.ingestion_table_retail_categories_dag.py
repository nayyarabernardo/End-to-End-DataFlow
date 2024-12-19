from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Adiciona o caminho relativo ao Python Path
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts/ingestion_table/'))


# Importa as funções do script
from ingestion_table_retail_categories import (
    setup_bigquery_credentials,
    get_most_recent_file_from_log,
    verify_and_create_tables,
    load_data_to_raw,
    merge_raw_to_trusted,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
}

with DAG(
    'load_to_bigquery_dag',
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
        task_id='get_most_recent_file_from_log',
        python_callable=get_most_recent_file_from_log,
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

