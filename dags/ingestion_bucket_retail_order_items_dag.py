from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import storage
import os
from ingestion_bucket_retail_V3 import (
    list_files_in_bucket,
    list_local_files,
    compare_and_filter_files,
    upload_new_files
)

# 
ORIGEM = "retail_db"
NAME = "order_items" 
PROJECT_ID = "bankmarketingdatapipeline"
BUCKET_NAME = "ingestion-raw-data-retail"
PREFIX = "order_items/"


# Configuração da DAG
default_args = {    
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
}

with DAG(
    f'3-load_bucket_file_{NAME}',
    default_args=default_args,
    description=f'Pipeline para carga no CSV {NAME}',
    schedule_interval='0 3 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task para listar arquivos no bucket
    list_buckets = PythonOperator(
        task_id='Lista_de_Buckets_existentes',
        python_callable=list_files_in_bucket,
        op_kwargs={
            'gsclient': storage.Client(project=PROJECT_ID),  
            'bucket_name': BUCKET_NAME,
            'prefix': PREFIX
        }
    )

    # Task para listar arquivos locais
    list_local_file = PythonOperator(
        task_id='Lista_de_arquivos_locais',
        python_callable=list_local_files,
        op_kwargs={
            'origem': ORIGEM,
            'name': NAME
        }
    )

    compare_filter_files = PythonOperator(
        task_id='Compara_e_filtra_arquivos',
        python_callable=compare_and_filter_files,
        op_kwargs={
            'bucket_files': "{{ ti.xcom_pull(task_ids='Lista_de_Buckets_existentes') }}",
            'local_files': "{{ ti.xcom_pull(task_ids='Lista_de_arquivos_locais') }}",
            'prefix': PREFIX
        }
    )

    upload_new_file = PythonOperator(
        task_id='Carrega_novos_arquivos',
        python_callable=upload_new_files,
        op_kwargs={
            'gsclient': storage.Client(project=PROJECT_ID),
            'bucket_name': BUCKET_NAME,
            'prefix': PREFIX,
            'origem': ORIGEM,
            'name': NAME,
            'files_to_upload': "{{ ti.xcom_pull(task_ids='Compara_e_filtra_arquivos') }}"
        }
    )

    list_buckets >> list_local_file >> compare_filter_files >> upload_new_file
