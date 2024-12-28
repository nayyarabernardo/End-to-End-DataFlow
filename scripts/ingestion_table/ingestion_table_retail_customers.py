import os
import logging
import re
from datetime import datetime
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

# Configuração centralizada do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Configurações do projeto
PROJECT_ID = "bankmarketingdatapipeline"
DATASET_NAME = "db_retail"
RAW_TABLE_NAME = "raw_customers"
TRUSTED_TABLE_NAME = "trusted_customers"
BUCKET_NAME = "ingestion-raw-data-retail"
PREFIX = "customers/"

def setup_bigquery_credentials():
    """Configura as credenciais do BigQuery"""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../../key.json"

def get_most_recent_file(bucket_name: str, prefix: str) -> str:
    """
    Encontra o arquivo mais recente em um bucket do GCS.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    pattern = re.compile(r"customers_(\d{14})")
    latest_file = None
    latest_datetime = None

    for blob in blobs:
        match = pattern.search(blob.name)
        if match:
            file_datetime = datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
            if latest_datetime is None or file_datetime > latest_datetime:
                latest_datetime = file_datetime
                latest_file = blob.name

    if latest_file:
        latest_file_uri = f"gs://{bucket_name}/{latest_file}"
        logger.info(f"✅ Arquivo mais recente encontrado: {latest_file_uri}")
        return latest_file_uri
    else:
        raise FileNotFoundError("Nenhum arquivo no formato esperado foi encontrado.")

def verify_and_create_tables(client: bigquery.Client, dataset_id: str, raw_table_id: str, trusted_table_id: str, uri: str):
    """
    Verifica se as tabelas raw e trusted existem. 
    Se não existirem, cria ambas e carrega os dados iniciais.
    """
    def table_exists(table_id: str) -> bool:
        try:
            client.get_table(table_id)
            logger.info(f"🔍 Tabela encontrada: {table_id}")
            return True
        except NotFound:
            logger.warning(f"⚠️ Tabela não encontrada: {table_id}")
            return False

    # Verificar e criar as tabelas se necessário
    raw_exists = table_exists(raw_table_id)
    trusted_exists = table_exists(trusted_table_id)

    if not raw_exists or not trusted_exists:

        
        job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(
                "customer_id", 
                "INTEGER", 
                mode="REQUIRED", 
                description="ID único do cliente"
            ),
            bigquery.SchemaField(
                "customer_fname", 
                "STRING", 
                mode="REQUIRED", 
                description="Primeiro nome do cliente"
            ),
            bigquery.SchemaField(
                "customer_lname", 
                "STRING", 
                mode="REQUIRED", 
                description="Sobrenome do cliente"
            ),
            bigquery.SchemaField(
                "customer_email", 
                "STRING", 
                mode="REQUIRED", 
                description="Endereço de e-mail do cliente"
            ),
            bigquery.SchemaField(
                "customer_password", 
                "STRING", 
                mode="REQUIRED", 
                description="Senha de acesso do cliente"
            ),
            bigquery.SchemaField(
                "customer_street", 
                "STRING", 
                mode="REQUIRED", 
                description="Endereço da rua onde o cliente reside"
            ),
            bigquery.SchemaField(
                "customer_city", 
                "STRING", 
                mode="REQUIRED", 
                description="Cidade onde o cliente reside"
            ),
            bigquery.SchemaField(
                "customer_state", 
                "STRING", 
                mode="REQUIRED", 
                description="Estado onde o cliente reside"
            ),
            bigquery.SchemaField(
                "customer_zipcode", 
                "STRING", 
                mode="REQUIRED", 
                description="Código postal do cliente"
            )
            ]
        )

        
        for table_id in [raw_table_id, trusted_table_id]:
            load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
            load_job.result()
            logger.info(f"✅ Tabela criada e dados carregados: {table_id}")

        # Obter a tabela
        table = client.get_table(table_id)

        # Atualizar descrição da tabela
        table.description = "Tabela contendo os clientes confiáveis do banco de dados de varejo."
        table = client.update_table(table, ["description"])

        # Pipeline termina aqui se as tabelas foram criadas
        logger.info("🏁 Tabelas criadas. Pipeline finalizado.")
        return True
    return False

def load_data_to_raw(client: bigquery.Client, raw_table_id: str, uri: str):
    """
    Carrega os dados mais recentes para a tabela RAW.
    """

    query = f"""
    DROP TABLE `{raw_table_id}`
    """
    logger.info(f"🧹 Removendo dados mais antigos que {raw_table_id} dias da tabela RAW.")
    client.query(query).result()

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(
                "customer_id", 
                "INTEGER", 
                mode="REQUIRED", 
                description="ID único do cliente"
            ),
            bigquery.SchemaField(
                "customer_fname", 
                "STRING", 
                mode="REQUIRED", 
                description="Primeiro nome do cliente"
            ),
            bigquery.SchemaField(
                "customer_lname", 
                "STRING", 
                mode="REQUIRED", 
                description="Sobrenome do cliente"
            ),
            bigquery.SchemaField(
                "customer_email", 
                "STRING", 
                mode="REQUIRED", 
                description="Endereço de e-mail do cliente"
            ),
            bigquery.SchemaField(
                "customer_password", 
                "STRING", 
                mode="REQUIRED", 
                description="Senha de acesso do cliente"
            ),
            bigquery.SchemaField(
                "customer_street", 
                "STRING", 
                mode="REQUIRED", 
                description="Endereço da rua onde o cliente reside"
            ),
            bigquery.SchemaField(
                "customer_city", 
                "STRING", 
                mode="REQUIRED", 
                description="Cidade onde o cliente reside"
            ),
            bigquery.SchemaField(
                "customer_state", 
                "STRING", 
                mode="REQUIRED", 
                description="Estado onde o cliente reside"
            ),
            bigquery.SchemaField(
                "customer_zipcode", 
                "STRING", 
                mode="REQUIRED", 
                description="Código postal do cliente"
            )
            ]
        )
    
 
    for table_id in [raw_table_id]:
            load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
            load_job.result()
            logger.info(f"✅ Tabela criada e dados carregados: {table_id}")


    
    logger.info(f"✅ Dados carregados na tabela RAW: {raw_table_id}")

def merge_raw_to_trusted(client: bigquery.Client, raw_table_id: str, trusted_table_id: str):
    """
    Faz o merge entre a tabela RAW e a tabela TRUSTED.
    """
    merge_query = f"""
    MERGE `{trusted_table_id}` T
    USING `{raw_table_id}` R
    ON T.customer_id = R.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            customer_fname = R.customer_fname,
            customer_lname = R.customer_lname,
            customer_email = R.customer_email,
            customer_password = R.customer_password,
            customer_street = R.customer_street,
            customer_city = R.customer_city,
            customer_state = R.customer_state,
            customer_zipcode = R.customer_zipcode
    WHEN NOT MATCHED THEN
        INSERT (customer_id, customer_fname, customer_lname, customer_email, customer_password, customer_street, customer_city, customer_state, customer_zipcode)
        VALUES (R.customer_id, R.customer_fname, R.customer_lname, R.customer_email, R.customer_password, R.customer_street, R.customer_city, R.customer_state, R.customer_zipcode)
    """
    query_job = client.query(merge_query)
    query_job.result()
    logger.info(f"✅ Merge concluído: RAW -> TRUSTED")

def main():
    """Função principal que orquestra o pipeline."""
    setup_bigquery_credentials()
    client = bigquery.Client(project=PROJECT_ID)

    dataset_id = f"{PROJECT_ID}.{DATASET_NAME}"
    raw_table_id = f"{dataset_id}.{RAW_TABLE_NAME}"
    trusted_table_id = f"{dataset_id}.{TRUSTED_TABLE_NAME}"

    prefix = "customers/"
    # Obter o arquivo mais recente
    try:
        uri = get_most_recent_file(BUCKET_NAME, PREFIX)
    except FileNotFoundError as e:
        logger.error(f"❌ Erro ao buscar o arquivo mais recente: {e}")
        return

    # 1. Verificar/criar tabelas
    tables_created = verify_and_create_tables(client, dataset_id, raw_table_id, trusted_table_id, uri)
    if tables_created:
        return  # Pipeline termina se as tabelas foram criadas e os dados carregados

    # 2. Carregar dados na tabela RAW
    load_data_to_raw(client, raw_table_id, uri)

    # 3. Merge entre RAW e TRUSTED
    merge_raw_to_trusted(client, raw_table_id, trusted_table_id)

if __name__ == "__main__":
    main()
