from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery,storage
from ingestion_table_retail_V2 import (
    get_most_recent_file,
    verify_and_create_tables,
    load_data_to_raw,
    merge_raw_to_trusted
)

 
# Configurações específicas para a tabela "orders"
NAME = "orders"
PROJECT_ID = "bankmarketingdatapipeline"
DATASET_NAME = "db_retail"
BUCKET_NAME = "ingestion-raw-data-retail"
PREFIX = "orders/"
#TABLE_RAW_NAME = "raw_orders"
#TABLE_TRUSTED_NAME = "trusted_orders"
PONTEIRO = "order_id"
TABLE_DESCRIPTION = "Tabela contendo os pedidos confiáveis do banco de dados de varejo."

# Esquema de tabelas 

SCHEMA=[
    bigquery.SchemaField("order_id", "INTEGER", mode="REQUIRED", description="ID único do pedido"),
    bigquery.SchemaField("order_date",  "TIMESTAMP", mode="REQUIRED", description="Data e hora em que o pedido foi realizado"),
    bigquery.SchemaField( "order_customer_id", "INTEGER", mode="REQUIRED", description="ID do cliente que fez o pedido"),
    bigquery.SchemaField( "order_status", "STRING", mode="REQUIRED", description="Status atual do pedido (ex: 'em processamento', 'enviado')")
]



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
}

with DAG(
    f'2-load_table_trusted_{NAME}_v8',
    default_args=default_args,
    description=f'Pipeline para carga no BigQuery para {NAME}',
    schedule_interval='0 4 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    get_recent_file = PythonOperator(
        task_id='get_most_recent_file',
        python_callable=get_most_recent_file,
        op_kwargs={
            'gsclient': storage.Client(project=PROJECT_ID), 
            'name': NAME,
            'bucket_name': BUCKET_NAME,
            'prefix': PREFIX            
        }
    )

    verify_create_tables = PythonOperator(
        task_id='verify_and_create_tables',
        python_callable=verify_and_create_tables,
        op_kwargs={
            'client': bigquery.Client(project=PROJECT_ID), 
            'projeto': PROJECT_ID,
            'dataset':DATASET_NAME,
            'name': NAME,
            'uri': "{{ task_instance.xcom_pull(task_ids='get_most_recent_file') }}",  
            'schema': SCHEMA,
            'table_description': TABLE_DESCRIPTION
        },
        provide_context=True  # Inclui contexto para debug
    )

    load_raw = PythonOperator(
        task_id='load_data_to_raw',
        python_callable=load_data_to_raw,
        op_kwargs={
            'client': bigquery.Client(project=PROJECT_ID),
            'projeto': PROJECT_ID,
            'dataset':DATASET_NAME,
            'name': NAME,
            'uri': "{{ task_instance.xcom_pull(task_ids='get_most_recent_file') }}",  # URI obtido na task anterior
            'schema': SCHEMA
        }
    )

    merge_data = PythonOperator(
        task_id='merge_raw_to_trusted',
        python_callable=merge_raw_to_trusted,
        op_kwargs={
            'client': bigquery.Client(project=PROJECT_ID),
            'projeto': PROJECT_ID,
            'dataset':DATASET_NAME,
            'name': NAME,
            'ponteiro': PONTEIRO,
            'schema': SCHEMA
        }
    )

    # Definir a ordem de execução das tasks
    get_recent_file >> verify_create_tables >> load_raw >> merge_data
