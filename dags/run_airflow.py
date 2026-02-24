from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys


sys.path.append('/opt/airflow')

from scripts.extraction import Extraction, URL, OUT_JSON_NAME, OUT_CSV_NAME
from scripts.to_s3 import CargaS3
from scripts.to_snowflake import SnowflakeConnector

default_args = {
    'owner': 'stori_challenge',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_step():
    extractor = Extraction(URL, OUT_JSON_NAME, OUT_CSV_NAME)
    extractor.connect_and_save()
    extractor.flatten_json()
    return OUT_CSV_NAME  

def upload_to_s3_step(ti):
    file_to_upload = ti.xcom_pull(task_ids='extract_data')
    uploader = CargaS3(s3_bucket=os.getenv("BUCKET_NAME"), folder="raw")
    uploader.upload(file_to_upload)

def load_to_snowflake_step():
    sf = SnowflakeConnector(
        user=os.getenv('SNOWFLAKE_USER'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    try:
        sf.ingest_from_stage(
            os.getenv("SNOWFLAKE_TABLE_NAME"), 
            os.getenv('SNOWFLAKE_STAGE_NAME')
        )
    finally:
        sf.close()

with DAG(
    'stori_data_pipeline',
    default_args=default_args,
    description='Pipeline de ingesta API -> S3 -> Snowflake',
    schedule_interval='@daily',
    catchup=False, 
) as dag:

    t1 = PythonOperator(
        task_id='extract_data',
        python_callable=extract_step
    )

    t2 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3_step
    )

    t3 = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake_step
    )

    t1 >> t2 >> t3