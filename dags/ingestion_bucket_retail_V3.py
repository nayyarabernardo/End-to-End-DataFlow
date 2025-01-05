from google.cloud import storage
import os
from typing import List
import ast


def list_files_in_bucket(gsclient: storage.Client, bucket_name: str, prefix: str) -> List[str]:
    """ Lista todos os arquivos CSV dentro de uma pasta específica em um bucket no Google Cloud Storage. """
    try:
        bucket = gsclient.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        # Extrai apenas os nomes dos arquivos, removendo o prefixo 'products/'.
        bucket_files = [blob.name.replace(prefix, '') for blob in blobs if blob.name.endswith('.csv')]
        
        print(f"Arquivos encontrados no bucket '{bucket_name}' com prefixo '{prefix}':")
        for file in bucket_files:
            print(f" - {file}")
        
        return bucket_files
    except Exception as e:
        print(f"Erro ao listar arquivos: {e}")
        return []


def list_local_files(origem: str, name: str) -> List[str]:
    """
    Lista todos os arquivos CSV dentro de uma pasta local.
    """
    folder_path = f"/opt/airflow/data/{origem}/{name}"
    
    print(f"Verificando arquivos locais na pasta '{folder_path}'...")
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"A pasta local '{folder_path}' não existe.")
    
    local_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]
    print(f"Arquivos CSV encontrados localmente:")
    for file in local_files:
        print(f"   - {file}")
    return local_files

def compare_and_filter_files(bucket_files, local_files):
    
    if isinstance(bucket_files, str):
        try:
            bucket_files = ast.literal_eval(bucket_files)
        except:
            print("Erro ao converter bucket_files")
            return []
    
    if isinstance(local_files, str):
        try:
            local_files = ast.literal_eval(local_files)
        except:
            print("Erro ao converter local_files")
            return []
    
    files_to_upload = [f for f in local_files if f not in bucket_files]
    
    print(f"\nArquivos que precisam ser enviados para o bucket: {files_to_upload}")
    
    return files_to_upload

def upload_new_files(gsclient: storage.Client, bucket_name: str, prefix: str ,origem: str, name: str, files_to_upload: List[str]):
    """
    Faz o upload dos arquivos que estão faltando para o bucket.
    """
    if isinstance(files_to_upload, str):
        try:
            files_to_upload = ast.literal_eval(files_to_upload)
        except:
            print("Erro ao converter files_to_upload")
            return []
        
    print(f"Iniciando upload de {len(files_to_upload)} arquivo(s) para o bucket '{bucket_name}'...")
    bucket = gsclient.bucket(bucket_name)
    
    folder_path = f"/opt/airflow/data/{origem}/{name}"

    for file in files_to_upload:
        local_path = os.path.join(folder_path, os.path.basename(file))
        blob_name = f"{prefix}{os.path.basename(file)}"
        
        if os.path.exists(local_path):
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(local_path)
            print(f"Arquivo '{local_path}' enviado para '{blob_name}'.")
        else:
            print(f"⚠️ Arquivo local '{local_path}' não encontrado. Pulando.")
    
    print("🎉 Upload concluído!")
