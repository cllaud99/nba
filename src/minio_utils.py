import os
import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from crawler_nba_api import fetch_api_response
from io import StringIO

class MinioManager:
    def __init__(self):
        self.endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        self.access_key = os.getenv("MINIO_ROOT_USER", "admin-minio")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD", "admin-minio")
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.s3_client = self.create_s3_client()

    def create_s3_client(self):
        """
        Cria um cliente boto3 configurado para MinIO.

        Returns:
            boto3.client: Cliente do S3 configurado para MinIO.
        """
        session = boto3.session.Session()
        return session.client(
            service_name="s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
        )

    def upload_csv_to_minio(self, csv_file_path, object_name, bucket_name):
        """
        Faz o upload de um arquivo CSV para um bucket no MinIO.

        Args:
            csv_file_path (str): Caminho para o arquivo CSV a ser carregado.
            object_name (str): Nome do objeto no bucket do MinIO.
            bucket_name (str): Nome do bucket onde o arquivo será armazenado.
        """
        # Ler o arquivo CSV
        df = pd.read_csv(csv_file_path)

        # Salvar o DataFrame como CSV em um arquivo temporário
        temp_csv_path = "/tmp/temp_file.csv"
        df.to_csv(temp_csv_path, index=False)

        # Verificar e criar o bucket se necessário
        self.ensure_bucket_exists(bucket_name)

        # Fazer upload do arquivo CSV para o MinIO
        try:
            self.s3_client.upload_file(temp_csv_path, bucket_name, object_name)
            print(f"Arquivo '{object_name}' enviado com sucesso para o bucket '{bucket_name}'")
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Erro de credenciais: {e}")
        except Exception as e:
            print(f"Erro ao enviar o arquivo: {e}")

    def ensure_bucket_exists(self, bucket_name):
        """
        Verifica se o bucket existe e o cria se não existir.

        Args:
            bucket_name (str): Nome do bucket a ser verificado/criado.
        """
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' já existe")
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.s3_client.create_bucket(Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' criado com sucesso")
            else:
                raise

    def read_csv_from_minio(self, object_name, bucket_name):
        """
        Lê um arquivo CSV do bucket no MinIO e retorna um DataFrame.

        Args:
            object_name (str): Nome do objeto no bucket do MinIO.
            bucket_name (str): Nome do bucket onde o arquivo está armazenado.

        Returns:
            DataFrame: Dados lidos do arquivo CSV.
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=object_name)
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

    def list_objects_in_bucket(self, bucket_name):
        """
        Lista todos os objetos em um bucket no MinIO.

        Args:
            bucket_name (str): Nome do bucket cujos objetos serão listados.

        Returns:
            list: Lista de objetos no bucket.
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)
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

    def process_data(self, season: str, season_type: str, per_mode: str, object_name: str, bucket_name: str):
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

        self.upload_csv_to_minio(temp_csv_path, object_name, bucket_name)

        # Ler o CSV do MinIO (opcional)
        df_from_minio = self.read_csv_from_minio(object_name, bucket_name)
        if df_from_minio is not None:
            print(df_from_minio.head())