import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from crawler_nba_api import fetch_api_response


def upload_csv_to_minio(csv_file_path, object_name, bucket_name):
    """
    Faz o upload de um arquivo CSV para um bucket no MinIO.

    Args:
        csv_file_path (str): Caminho para o arquivo CSV a ser carregado.
        object_name (str): Nome do objeto no bucket do MinIO.
        bucket_name (str): Nome do bucket onde o arquivo será armazenado.
    """
    # Carregar variáveis de ambiente
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER", "admin-minio")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "admin-minio")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    # Criar uma sessão boto3 configurada para MinIO
    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        region_name=aws_region,
    )

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
        print(
            f"Arquivo '{object_name}' enviado com sucesso para o bucket '{bucket_name}'"
        )
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Erro de credenciais: {e}")
    except Exception as e:
        print(f"Erro ao enviar o arquivo: {e}")


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

if __name__ == "__main__":
    # Defina os parâmetros de exemplo ou configure-os conforme necessário
    csv_file_path = "./nba_stats/nba__1997-98_All Star.csv"
    object_name = "nba__1997-98_All Star.csv"
    bucket_name = "nba-raw"

    upload_csv_to_minio(csv_file_path, object_name, bucket_name)
