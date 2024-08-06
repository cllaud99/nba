import os
import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from crawler_nba_api import fetch_api_response
from io import StringIO

# Definindo variáveis de ambiente globalmente
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin-minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "admin-minio")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

def create_s3_client():
    """
    Cria um cliente boto3 configurado para MinIO.

    Returns:
        boto3.client: Cliente do S3 configurado para MinIO.
    """
    session = boto3.session.Session()
    return session.client(
        service_name="s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=AWS_REGION,
    )

def upload_csv_to_minio(csv_file_path, object_name, bucket_name):
    """
    Faz o upload de um arquivo CSV para um bucket no MinIO.

    Args:
        csv_file_path (str): Caminho para o arquivo CSV a ser carregado.
        object_name (str): Nome do objeto no bucket do MinIO.
        bucket_name (str): Nome do bucket onde o arquivo será armazenado.
    """
    s3_client = create_s3_client()

    # Ler o arquivo CSV
    df = pd.read_csv(csv_file_path)

    # Salvar o DataFrame como CSV em um arquivo temporário
    temp_csv_path = "/tmp/temp_file.csv"
    df.to_csv(temp_csv_path, index=False)

    def ensure_bucket_exists(client, bucket_name):
        """
        Verifica se o bucket existe e o cria se não existir.

        Parâmetros:
        client (boto3.client): Cliente do S3.
        bucket_name (str): Nome do bucket a ser verificado/criado.
        """
        try:
            client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' já existe")
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                client.create_bucket(Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' criado com sucesso")
            else:
                raise

    # Verificar e criar o bucket se necessário
    ensure_bucket_exists(s3_client, bucket_name)

    # Fazer upload do arquivo CSV para o MinIO
    try:
        s3_client.upload_file(temp_csv_path, bucket_name, object_name)
        print(f"Arquivo '{object_name}' enviado com sucesso para o bucket '{bucket_name}'")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Erro de credenciais: {e}")
    except Exception as e:
        print(f"Erro ao enviar o arquivo: {e}")

def read_csv_from_minio(object_name, bucket_name):
    """
    Lê um arquivo CSV do bucket no MinIO e retorna um DataFrame.

    Args:
        object_name (str): Nome do objeto no bucket do MinIO.
        bucket_name (str): Nome do bucket onde o arquivo está armazenado.

    Returns:
        DataFrame: Dados lidos do arquivo CSV.
    """
    s3_client = create_s3_client()

    # Ler o CSV do MinIO
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            csv_content = response["Body"].read().decode('utf-8')
            return pd.read_csv(StringIO(csv_content))
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
            return None
    except ClientError as e:
        print(f"Erro ao ler o arquivo: {e}")
        return None
    
def list_objects_in_bucket(bucket_name):
    """
    Lista todos os objetos em um bucket no MinIO.

    Args:
        bucket_name (str): Nome do bucket cujos objetos serão listados.

    Returns:
        list: Lista de objetos no bucket.
    """
    s3_client = create_s3_client()

    try:
        # Lista os objetos no bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        
        if status == 200:
            print(f"Successful S3 list_objects_v2 response. Status - {status}")
            objects = response.get("Contents", [])
            if not objects:
                print("Nenhum objeto encontrado no bucket.")
            else:
                for obj in objects:
                    print(f"Nome do objeto: {obj['Key']}, Tamanho: {obj['Size']} bytes")
            return objects
        else:
            print(f"Unsuccessful S3 list_objects_v2 response. Status - {status}")
            return None
    except ClientError as e:
        print(f"Erro ao listar objetos: {e}")
        return None

def main(season: str, season_type: str, per_mode: str, object_name: str, bucket_name: str):
    """
    Função principal para buscar dados da API, salvar em CSV e enviar para o MinIO.

    Args:
        season (str): Temporada para a qual a requisição será feita.
        season_type (str): Tipo de temporada (ex: "Regular Season").
        per_mode (str): Modo de métrica (ex: 'PerGame').
        object_name (str): Nome do objeto no bucket do MinIO.
        bucket_name (str): Nome do bucket onde o arquivo será armazenado.
    """
    data = fetch_api_response(season, season_type, per_mode)
    df = pd.DataFrame(data['resultSets'][0]['rowSet'], columns=data['resultSets'][0]['headers'])
    
    temp_csv_path = "/tmp/nba_data.csv"
    df.to_csv(temp_csv_path, index=False)

    upload_csv_to_minio(temp_csv_path, object_name, bucket_name)
    
    # Ler o CSV do MinIO (opcional)
    df_from_minio = read_csv_from_minio(object_name, bucket_name)
    if df_from_minio is not None:
        print(df_from_minio.head())