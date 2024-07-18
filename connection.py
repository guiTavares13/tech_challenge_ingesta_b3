import boto3
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())

class Connection:
    def __init__(self):
       self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
            aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
            region_name=os.getenv('AWS_REGION')
        )

    def upload_file_to_s3(self, file_path, bucket_name, s3_key):
        # Faz o upload do arquivo
        try:
            response = self.s3_client.upload_file(file_path, bucket_name, s3_key)
            print(f"Upload bem-sucedido: {response}")
            return True
        except Exception as e:
            print(f"Erro ao fazer upload: {e}")
            return False
