import boto3
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

load_dotenv() 


AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
REGION_NAME = os.getenv("REGION_NAME")

logging.basicConfig(
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class CargaS3: 
    def __init__(self, s3_bucket: str, folder: str) -> None:  
        self.s3_bucket = s3_bucket
        self.folder = folder
        self.s3_client = boto3.client('s3',
                                        aws_access_key_id=AWS_ACCESS_KEY,
                                            aws_secret_access_key=AWS_SECRET_KEY,
                                                region_name=REGION_NAME)
        logger.info(f"Comenzando proceso para el bucket: {self.s3_bucket}")

    def upload(self, local_file_path: str) -> str:
        now = datetime.now()
        file_name = os.path.basename(local_file_path)
        s3_path = f"{self.folder}/{now.year}/{now.month:02d}/{now.day:02d}/{file_name}"
        try:
            self.s3_client.upload_file(local_file_path, self.s3_bucket, s3_path)
            logger.info(f"Ok, archivo en: s3://{self.s3_bucket}/{s3_path}")
            return s3_path
        except Exception as e:
            logger.error(f"Error en carga a S3: {e}")

if __name__ == '__main__':
    
    BUCKET = os.getenv("BUCKET_NAME")
    INPUT_PATH = os.getenv("INPUT_PATH")
    uploader = CargaS3(s3_bucket=BUCKET, folder="raw")
    uploader.upload(INPUT_PATH)
    
