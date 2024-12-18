import os
import logging
from google.cloud import storage
from typing import List, Optional

def setup_gcs_credentials(
    credentials_path: Optional[str] = None, 
    credentials_env_var: str = "GOOGLE_APPLICATION_CREDENTIALS",
    verbose: bool = True
) -> storage.Client:
    """
    Configura as credenciais do Google Cloud Storage e inicializa o cliente.
    
    Args:
        credentials_path (str, opcional): Caminho para o arquivo de credenciais JSON
        credentials_env_var (str, opcional): Nome da variável de ambiente para credenciais
        verbose (bool, opcional): Exibir logs detalhados
    
    Returns:
        storage.Client: Cliente do Google Cloud Storage
    """
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format='%(asctime)s - %(levelname)s: %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        # Se o caminho não for fornecido, tenta usar uma localização padrão
        if credentials_path is None:
            default_paths = [
                "../key.json",
                "./key.json",
                os.path.expanduser("~/key.json")
            ]
            
            for path in default_paths:
                if os.path.exists(path):
                    credentials_path = path
                    break

        # Validar existência do arquivo de credenciais
        if not credentials_path or not os.path.exists(credentials_path):
            raise FileNotFoundError(f"Arquivo de credenciais não encontrado em {credentials_path}")

        # Configurar variável de ambiente
        os.environ[credentials_env_var] = credentials_path
        logger.info(f"Credenciais configuradas: {credentials_path}")

        # Inicializar cliente do Google Cloud Storage
        gsclient = storage.Client()
        
        return gsclient

    except Exception as e:
        logger.error(f"Erro ao configurar credenciais: {e}")
        raise

def list_gcs_buckets(
    gsclient: Optional[storage.Client] = None, 
    verbose: bool = True
) -> List[str]:
    """
    Lista os buckets disponíveis no Google Cloud Storage.
    
    Args:
        gsclient (storage.Client, opcional): Cliente do GCS. Se None, será criado.
        verbose (bool, opcional): Exibir logs detalhados
    
    Returns:
        List[str]: Lista de nomes dos buckets
    """
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format='%(asctime)s - %(levelname)s: %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        # Criar cliente se não fornecido
        if gsclient is None:
            gsclient = setup_gcs_credentials()

        # Listar buckets
        buckets = list(gsclient.list_buckets())
        bucket_names = [bucket.name for bucket in buckets]

        if verbose:
            logger.info(f"Buckets encontrados ({len(bucket_names)}):")
            for name in bucket_names:
                logger.info(f"- {name}")

        return bucket_names

    except Exception as e:
        logger.error(f"Erro ao listar buckets: {e}")
        return []
def create_gcs_bucket(
    bucket_name,
    location,
    storage_class,
    uniform_access,
    verbose,
):
    """
    Cria um bucket no Google Cloud Storage com configurações personalizáveis.
    
    Args:
        bucket_name (str): Nome do bucket a ser criado
        location (str, opcional): Localização do bucket. Padrão é 'us-east1'
        storage_class (str, opcional): Classe de armazenamento. Padrão é 'STANDARD'
        uniform_access (bool, opcional): Ativar acesso uniforme ao nível do bucket. Padrão é True
        verbose (bool, opcional): Exibir logs detalhados. Padrão é True
    
    Returns:
        google.cloud.storage.Bucket: Objeto do bucket criado
    """
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format='%(asctime)s - %(levelname)s: %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        # Verificar se o bucket já existe
        if gsclient.lookup_bucket(bucket_name):
            logger.warning(f"Bucket {bucket_name} já existe. Pulando criação.")
            return gsclient.get_bucket(bucket_name)

        # Configurações do bucket
        bucket_config = {
            "location": location,
            "storage_class": storage_class
        }

        # Criar o bucket
        bucket = gsclient.create_bucket(
            bucket_name, 
            location=bucket_config["location"]
        )

        # Definir classe de armazenamento
        bucket.storage_class = bucket_config["storage_class"]
        bucket.patch()

        # Configurar acesso uniforme (opcional)
        if uniform_access:
            bucket.iam_configuration.uniform_bucket_level_access_enabled = True
            bucket.patch()
            logger.info(f"Acesso uniforme ativado para o bucket {bucket.name}.")

        logger.info(f"Bucket {bucket.name} criado com sucesso!")
        logger.info(f"Localização: {bucket.location}")
        logger.info(f"Classe de Armazenamento: {bucket.storage_class}")

        return bucket

    except Exception as e:
        logger.error(f"Erro ao criar bucket: {e}")
        raise

def create_gcs_folder(bucket_name: str, folder_name: str, verbose: bool = True):
    """
    Cria uma pasta lógica em um bucket do GCS, se ainda não existir.
    
    Args:
        bucket_name (str): Nome do bucket
        folder_name (str): Nome da pasta a ser criada
        verbose (bool, opcional): Exibir logs detalhados. Padrão é True.
    
    Returns:
        bool: True se a pasta foi criada, False se já existia.
    """
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format='%(asctime)s - %(levelname)s: %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        # Criar bucket cliente
        bucket = gsclient.bucket(bucket_name)

        # Verificar se a "pasta" já existe (prefixo presente)
        blobs = list(bucket.list_blobs(prefix=f"{folder_name}/"))
        if blobs:
            logger.info(f"A pasta lógica '{folder_name}/' já existe no bucket '{bucket_name}'.")
            return False

        # Criar pasta lógica como um blob vazio
        placeholder_blob = bucket.blob(f"{folder_name}/")
        placeholder_blob.upload_from_string("")  # Blob vazio

        logger.info(f"Pasta lógica '{folder_name}/' criada no bucket '{bucket_name}'.")
        return True

    except Exception as e:
        logger.error(f"Erro ao criar pasta lógica '{folder_name}/': {e}")
        raise


def upload_folder_to_gcs(local_folder: str, bucket_name: str, gcs_folder: str = "", verbose: bool = True):
    """
    Carrega recursivamente todos os arquivos de uma pasta local para um bucket do GCS.
    
    Args:
        local_folder (str): Caminho da pasta local a ser carregada
        bucket_name (str): Nome do bucket no GCS
        gcs_folder (str, opcional): Nome da pasta lógica no GCS. Padrão é raiz do bucket.
        verbose (bool, opcional): Exibir logs detalhados. Padrão é True.
    
    Returns:
        dict: Estatísticas de upload contendo total de arquivos, arquivos enviados e falhas.
    """
    try:
        # Configurar logging
        logging.basicConfig(
            level=logging.INFO if verbose else logging.WARNING,
            format='%(asctime)s - %(levelname)s: %(message)s'
        )
        logger = logging.getLogger(__name__)

        # Criar cliente do bucket
        bucket = gsclient.bucket(bucket_name)

        # Criar pasta lógica no GCS, se necessário
        if gcs_folder:
            create_gcs_folder(bucket_name, gcs_folder, verbose)

        # Contadores para acompanhamento
        total_files = 0
        uploaded_files = 0
        failed_files = 0

        # Iterar pelos arquivos na pasta local
        for root, _, files in os.walk(local_folder):
            for file in files:
                total_files += 1

                # Caminho completo do arquivo local
                local_file_path = os.path.join(root, file)

                # Caminho relativo no GCS
                relative_path = os.path.relpath(local_file_path, local_folder)
                gcs_file_path = os.path.join(gcs_folder, relative_path).replace("\\", "/")

                try:
                    # Criar o Blob no GCS
                    blob = bucket.blob(gcs_file_path)

                    # Subir o arquivo
                    blob.upload_from_filename(local_file_path)
                    uploaded_files += 1

                    if verbose:
                        logger.info(f"Arquivo enviado: gs://{bucket_name}/{gcs_file_path}")
                
                except Exception as file_error:
                    logger.error(f"Erro ao enviar arquivo {local_file_path}: {file_error}")
                    failed_files += 1

        # Resumo do upload
        logger.info(f"Upload concluído. Total de arquivos: {total_files}, Enviados: {uploaded_files}")

        return {
            'total_files': total_files,
            'uploaded_files': uploaded_files,
            'failed_files': failed_files
        }

    except Exception as e:
        logging.error(f"Erro durante o upload: {e}")
        return None

if __name__ == "__main__":
    try:
        # Configurar logs
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s'
        )
        logger = logging.getLogger(__name__)

        # 1. Configurar credenciais e criar cliente
        logger.info("🔐 Configurando credenciais do Google Cloud Storage")
        gsclient = setup_gcs_credentials(
            credentials_path="../../key.json",
            verbose=True
        )

        # 2. Listar buckets existentes
        logger.info("📋 Listando buckets existentes")
        buckets = list_gcs_buckets(gsclient, verbose=True)

        # 3. Criar novo bucket
        bucket_name = "raw_retail"
        logger.info(f"🪣 Criando bucket: {bucket_name}")
        created_bucket = create_gcs_bucket(
            bucket_name=bucket_name, 
            location="us-east1", 
            storage_class="STANDARD", 
            uniform_access=True, 
            verbose=True
        )
        # 4. Fazer upload de pasta
        local_folder = "../../data/retail_db/departments"
        gcs_folder = "departments"  # Pasta lógica no GCS

        logger.info(f"📤 Iniciando upload da pasta: {local_folder} para a pasta lógica {gcs_folder}")
        upload_stats = upload_folder_to_gcs(
            local_folder=local_folder, 
            bucket_name=bucket_name, 
            gcs_folder=gcs_folder, 
            verbose=True
        )

 
        # Resumo final
        logger.info("✅ Processo concluído com sucesso!")
        logger.info(f"Buckets encontrados: {len(buckets)}")
        logger.info(f"Bucket criado: {bucket_name}")
        logger.info(f"Arquivos no upload: Total={upload_stats['total_files']}, "
                    f"Enviados={upload_stats['uploaded_files']}, "
                    f"Falhas={upload_stats['failed_files']}")

    except Exception as e:
        logger.error(f"❌ Erro no processo: {e}")
        # Opcional: logging de rastreamento de erro completo
        logging.exception("Detalhes do erro:")

