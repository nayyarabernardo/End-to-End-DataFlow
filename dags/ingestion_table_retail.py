import os
import logging
import re
from datetime import datetime
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound


def get_most_recent_file(bucket_name: str, prefix: str) -> str:
    """Obtém o arquivo mais recente de um bucket GCS com base no prefixo."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    pattern = re.compile(r"categories_(\d{14})")
    latest_file = None
    latest_datetime = None

    for blob in blobs:
        match = pattern.search(blob.name)
        if match:
            file_datetime = datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
            if not latest_datetime or file_datetime > latest_datetime:
                latest_datetime = file_datetime
                latest_file = blob.name

    if latest_file:
        latest_file_uri = f"gs://{bucket_name}/{latest_file}"
        return latest_file_uri
    else:
        raise FileNotFoundError("Nenhum arquivo no formato esperado foi encontrado.")

def verify_and_create_tables(client: bigquery.Client, raw_table_id: str, trusted_table_id: str, uri: str, schema: list, table_description: str):
    """
    Verifica se as tabelas raw e trusted existem. 
    Se não existirem, cria ambas e carrega os dados iniciais.
    """
    def table_exists(table_id: str) -> bool:
        try:
            client.get_table(table_id)
            return True
        except NotFound:
            return False

    # Verificar e criar as tabelas se necessário
    raw_exists = table_exists(raw_table_id)
    trusted_exists = table_exists(trusted_table_id)

    if not raw_exists or not trusted_exists:

        
        job_config = schema

        
        for table_id in [raw_table_id, trusted_table_id]:
            load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
            load_job.result()

        # Obter a tabela
        table = client.get_table(table_id)

        # Atualizar descrição da tabela
        table.description = table_description 
        table = client.update_table(table, ["description"])

        # Pipeline termina aqui se as tabelas foram criadas
        return True
    return False


def load_data_to_raw(client: bigquery.Client, raw_table_id: str, uri: str, schema: list):
    """Carrega os dados para a tabela RAW no BigQuery a partir do URI fornecido."""
    job_config = bigquery.LoadJobConfig(
        schema=schema,  # Usa o esquema correto
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
    )

    load_job = client.load_table_from_uri(
        uri, raw_table_id, job_config=job_config
    )

    load_job.result()
    print(f"Dados carregados para {raw_table_id}")

def merge_raw_to_trusted(client: bigquery.Client, raw_table_id: str, trusted_table_id: str, schema: list):
    """
    Faz o merge entre a tabela RAW e a tabela TRUSTED, de forma flexível com base no schema fornecido.
    """
    primary_key = schema[0].name  # Usando a primeira coluna como chave primária
    set_clause = ", ".join([f"T.{field.name} = R.{field.name}" for field in schema[1:]])  # Ignora a chave primária
    insert_columns = ", ".join([field.name for field in schema])
    insert_values = ", ".join([f"R.{field.name}" for field in schema])

    merge_query = f"""
    MERGE `{trusted_table_id}` T
    USING `{raw_table_id}` R
    ON T.{primary_key} = R.{primary_key}
    WHEN MATCHED THEN
        UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """
    
    query_job = client.query(merge_query)
    query_job.result()

