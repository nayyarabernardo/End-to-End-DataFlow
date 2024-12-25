from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery
from ingestion_table_retail import (
    get_most_recent_file,
    verify_and_create_tables,
    load_data_to_raw,
    merge_raw_to_trusted
)


NOME = "departments"
PROJECT_ID = "bankmarketingdatapipeline"
DATASET_NAME = "db_retail"
BUCKET_NAME = "ingestion-raw-data-retail"
PREFIX = "departments/"
TABLE_RAW_NAME = "raw_departments"
TABLE_TRUSTED_NAME = "trusted_departments"
TABLE_DESCRIPTION = "Tabela contendo os departamentos confiáveis do banco de dados de varejo."


SCHEMA = [
    bigquery.SchemaField("department_id", "INTEGER", mode="REQUIRED", description="ID único da departamento"),
    bigquery.SchemaField("department_name", "STRING", mode="REQUIRED", description="Nome da departamento")
]


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
}

with DAG(
    f'1-load_table_trusted_{NOME}',
    default_args=default_args,
    description=f'Pipeline para carga no BigQuery para {NOME}',
    schedule_interval='0 4 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    get_recent_file = PythonOperator(
        task_id='get_most_recent_file',
        python_callable=get_most_recent_file,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            'prefix': PREFIX
        }
    )

    verify_tables = PythonOperator(
        task_id='verify_and_create_tables',
        python_callable=verify_and_create_tables,
        op_kwargs={
            'client': bigquery.Client(project=PROJECT_ID),            
            'raw_table_id': f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_RAW_NAME}",
            'trusted_table_id': f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_TRUSTED_NAME}",
            'uri': "{{ task_instance.xcom_pull(task_ids='get_most_recent_file') }}",  # URI obtido na task anterior
            'schema': SCHEMA,
            'table_description': TABLE_DESCRIPTION
        }
    )

    load_raw = PythonOperator(
        task_id='load_data_to_raw',
        python_callable=load_data_to_raw,
        op_kwargs={
            'client': bigquery.Client(project=PROJECT_ID),
            'raw_table_id': f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_RAW_NAME}",
            'uri': "{{ task_instance.xcom_pull(task_ids='get_most_recent_file') }}",  # URI obtido na task anterior
            'schema': SCHEMA
        }
    )

    # Task para realizar o merge entre a tabela RAW e TRUSTED
    merge_data = PythonOperator(
        task_id='merge_raw_to_trusted',
        python_callable=merge_raw_to_trusted,
        op_kwargs={
            'client': bigquery.Client(project=PROJECT_ID),
            'raw_table_id': f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_RAW_NAME}",
            'trusted_table_id': f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_TRUSTED_NAME}",
            'schema': SCHEMA
        }
    )

    # Definir a ordem de execução das tasks
    get_recent_file >> verify_tables >> load_raw >> merge_data
