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
RAW_TABLE_NAME = "raw_order"
TRUSTED_TABLE_NAME = "trusted_order"
BUCKET_NAME = "raw_retail"
PREFIX = "order/"

def setup_bigquery_credentials():
    """Configura as credenciais do BigQuery"""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../../key.json"

def get_most_recent_file_from_log(log_file: str) -> str:
    """
    Encontra o arquivo mais recente com base em um log local de uploads.
    """
    try:
        with open(log_file, "r") as log:
            lines = log.readlines()
        
        if not lines:
            raise FileNotFoundError("O arquivo de log está vazio.")

        # Assume que cada linha do log segue o formato: "gs://bucket_name/prefix/orders_YYYYMMDDHHMMSS.csv"
        latest_file = None
        latest_datetime = None

        for line in lines:
            file_path = line.strip()
            match = re.search(r"orders_(\d{14})", file_path)
            if match:
                file_datetime = datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
                if latest_datetime is None or file_datetime > latest_datetime:
                    latest_datetime = file_datetime
                    latest_file = file_path

        if latest_file:
            logger.info(f"✅ Arquivo mais recente encontrado no log: {latest_file}")
            return latest_file
        else:
            raise FileNotFoundError("Nenhum arquivo no formato esperado foi encontrado no log.")
    except FileNotFoundError:
        raise FileNotFoundError("O arquivo de log não foi encontrado.")
    except Exception as e:
        logger.error(f"Erro ao ler o arquivo de log: {e}")
        raise


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
        #schema = [
        #    bigquery.SchemaField("order_id", "INTEGER", mode="REQUIRED"),
        #    bigquery.SchemaField("order_date", "TIMESTAMP", mode="REQUIRED"),
        #    bigquery.SchemaField("order_customer_id", "INTEGER", mode="REQUIRED"),
        #    bigquery.SchemaField("order_status", "STRING", mode="REQUIRED"),
        #]
        #job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
        
        job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(
                "order_id", 
                "INTEGER", 
                mode="REQUIRED", 
                description="ID único do pedido"
            ),
            bigquery.SchemaField(
                "order_date", 
                "TIMESTAMP", 
                mode="REQUIRED", 
                description="Data e hora em que o pedido foi realizado"
            ),
            bigquery.SchemaField(
                "order_customer_id", 
                "INTEGER", 
                mode="REQUIRED", 
                description="ID do cliente que fez o pedido"
            ),
            bigquery.SchemaField(
                "order_status", 
                "STRING", 
                mode="REQUIRED", 
                description="Status atual do pedido (ex: 'em processamento', 'enviado')"
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
        table.description = "Tabela contendo os pedidos confiáveis do banco de dados de varejo."
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
    logger.info("✅ Dados antigos removidos da tabela RAW.")

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(
                "order_id", 
                "INTEGER", 
                mode="REQUIRED", 
                description="ID único do pedido"
            ),
            bigquery.SchemaField(
                "order_date", 
                "TIMESTAMP", 
                mode="REQUIRED", 
                description="Data e hora em que o pedido foi realizado"
            ),
            bigquery.SchemaField(
                "order_customer_id", 
                "INTEGER", 
                mode="REQUIRED", 
                description="ID do cliente que fez o pedido"
            ),
            bigquery.SchemaField(
                "order_status", 
                "STRING", 
                mode="REQUIRED", 
                description="Status atual do pedido (ex: 'em processamento', 'enviado')"
            )
            ]
        )
    #load_job = client.load_table_from_uri(
    #    uri, raw_table_id, job_config=job_config
    #)  
    #load_job.result() 
    for table_id in [raw_table_id]:
            load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
            load_job.result()
            logger.info(f"✅ Tabela criada e dados carregados: {table_id}")

        # Obter a tabela

    #job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    #load_job = client.load_table_from_uri(uri, raw_table_id, job_config=job_config)
    #load_job.result()
    
    logger.info(f"✅ Dados carregados na tabela RAW: {raw_table_id}")

def merge_raw_to_trusted(client: bigquery.Client, raw_table_id: str, trusted_table_id: str):
    """
    Faz o merge entre a tabela RAW e a tabela TRUSTED.
    """
    merge_query = f"""
    MERGE `{trusted_table_id}` T
    USING `{raw_table_id}` R
    ON T.order_id = R.order_id
    WHEN MATCHED THEN
        UPDATE SET order_date = R.order_date,
                   order_customer_id = R.order_customer_id,
                   order_status = R.order_status
    WHEN NOT MATCHED THEN
        INSERT (order_id, order_date, order_customer_id, order_status)
        VALUES (R.order_id, R.order_date, R.order_customer_id, R.order_status)
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

    prefix = "orders/"
    # Obter o arquivo mais recente
    

    log_file = "../../logs/bucket/order_uploaded_files.log"

    try:
        most_recent_file = get_most_recent_file_from_log(log_file)
        # Remover barras extras ao concatenar
        uri = f"gs://{BUCKET_NAME}/{PREFIX.rstrip('/')}/{most_recent_file.lstrip('/')}"
        logger.info(f"Arquivo mais recente encontrado: {uri}")
    except FileNotFoundError as e:
        logger.error(f"Erro: {e}")
        return
    except Exception as e:
        logger.error(f"Ocorreu um erro inesperado: {e}")
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
