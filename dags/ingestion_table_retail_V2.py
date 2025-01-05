import os
import logging
import re
from datetime import datetime, timedelta
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound

def get_previous_business_day(reference_date=None):
    """Retorna o dia útil anterior à data de referência."""
    if reference_date is None:
        reference_date = datetime.utcnow()

    # Ajuste inicial para retornar ao dia anterior
    previous_day = reference_date - timedelta(days=1)

    # Caso seja final de semana (sábado ou domingo)
    if previous_day.weekday() == 5:  # Sábado
        previous_day -= timedelta(days=1)  # Volta para sexta
    elif previous_day.weekday() == 6:  # Domingo
        previous_day -= timedelta(days=2)  # Volta para sexta

    return previous_day

def get_most_recent_file(gsclient: storage.Client, name: str, bucket_name: str, prefix: str) -> str:
    """
    Obtém o arquivo mais recente de um bucket GCS com base no prefixo,
    garantindo que o arquivo seja do dia útil anterior.
    """
    bucket = gsclient.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    # Ajuste no padrão para incluir "order_" ou similar
    pattern = re.compile(f"{name}_(\d{{14}})\\.csv$")
    
    # Calcula o dia útil anterior
    previous_business_day = get_previous_business_day()
    target_date_str = previous_business_day.strftime("%Y%m%d")
    
    # Verifica o arquivo correspondente ao dia útil anterior
    for blob in blobs:
        match = pattern.search(blob.name)
        if match:
            file_datetime = datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
            # Verifica se a data do arquivo é do dia útil anterior
            if file_datetime.strftime("%Y%m%d") == target_date_str:
                latest_file_uri = f"gs://{bucket_name}/{blob.name}"
                print(f"Arquivo encontrado: {blob.name}")  # Debug: Exibe o nome do arquivo encontrado
                return latest_file_uri

    raise FileNotFoundError(f"Nenhum arquivo encontrado para o dia útil anterior ({target_date_str}).")


def create_table(client: bigquery.Client, table_id: str, uri: str, schema: list, description: str):
    """Cria uma tabela no BigQuery e carrega dados iniciais."""
    job_config = bigquery.LoadJobConfig(schema=schema)
    print(f"Criando tabela: {table_id}")
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()

    table = client.get_table(table_id)
    table.description = description
    client.update_table(table, ["description"])
    print(f"Tabela criada com sucesso: {table_id}")

    
def verify_and_create_tables(client: bigquery.Client, projeto: str, dataset: str, name: str, uri: str, schema: list, table_description: str):
    """Verifica e cria tabelas no BigQuery, se necessário."""
    def table_exists(table_id: str) -> bool:
        try:
            client.get_table(table_id)
            print(f"Tabela já existe: {table_id}")
            return True
        except NotFound:
            print(f"Tabela não encontrada: {table_id}")
            return False

    raw_table_id = f"{projeto}.{dataset}.raw_{name}"
    trusted_table_id = f"{projeto}.{dataset}.trusted_{name}"
    
    raw_exists = table_exists(raw_table_id)
    trusted_exists = table_exists(trusted_table_id)

    if not raw_exists:
        create_table(client, raw_table_id, uri, schema, table_description)

    if not trusted_exists:
        create_table(client, trusted_table_id, uri, schema, table_description)

    return not (raw_exists and trusted_exists)


def load_data_to_raw(client: bigquery.Client, projeto: str, dataset: str, name: str, uri: str, schema: list):
    """Carrega os dados para a tabela RAW no BigQuery a partir do URI fornecido."""
    job_config = bigquery.LoadJobConfig(
        schema=schema,  # Usa o esquema correto
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
    )

    raw_table_id = f"{projeto}.{dataset}.raw_{name}"
    
    load_job = client.load_table_from_uri(
        uri, raw_table_id, job_config=job_config
    )

    load_job.result()
    print(f"Dados carregados para {raw_table_id}")

def merge_raw_to_trusted(client: bigquery.Client, projeto: str, dataset: str, name: str, ponteiro: str, schema: list):
    """
    Faz o merge entre a tabela RAW e a tabela TRUSTED, de forma flexível com base no schema fornecido.
    """
    set_clause = ", ".join([f"T.{field.name} = R.{field.name}" for field in schema[1:]])  # Ignora a chave primária
    insert_columns = ", ".join([field.name for field in schema])
    insert_values = ", ".join([f"R.{field.name}" for field in schema])

    raw_table_id = f"{projeto}.{dataset}.raw_{name}"
    trusted_table_id = f"{projeto}.{dataset}.trusted_{name}"


    merge_query = f"""
    MERGE `{trusted_table_id}` T
    USING `{raw_table_id}` R
    ON T.{ponteiro} = R.{ponteiro}
    WHEN MATCHED THEN
        UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """
    
    query_job = client.query(merge_query)
    query_job.result()


    #Fazer query para que nao seja duplicado ele esta duplicando a hora de fazer a ingestao
    #Criar uma coluna na trusted e na raw com a data de ingestao e hora para ficar mais facil controle e analise 