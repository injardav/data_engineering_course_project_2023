import os
import zipfile
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from kaggle.api.kaggle_api_extended import KaggleApi

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_dataset():
    dataset_path = '/opt/airflow/data/arxiv.zip'

    # Check if the dataset already exists
    if not os.path.exists(dataset_path):
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files('Cornell-University/arxiv', path='/opt/airflow/data/', unzip=False)

def unzip_dataset():
    dataset_path = '/opt/airflow/data/arxiv.zip'
    extracted_path = '/opt/airflow/data/arxiv-metadata-oai-snapshot.json'

    # Check if the dataset zip file exists and if the extracted file does not exist
    if os.path.exists(dataset_path) and not os.path.exists(extracted_path):
        with zipfile.ZipFile(dataset_path, 'r') as zip_ref:
            zip_ref.extractall('/opt/airflow/data/')

def transform_and_save():
    file_path = '/opt/airflow/data/arxiv-metadata-oai-snapshot.json'
    output_path = '/opt/airflow/staging_area/arxiv_transformed.csv'

    if os.path.exists(file_path):
        df = pd.read_json(file_path, lines=True)
        df = df.dropna(subset=['doi'])
        df.to_csv(output_path, index=False)
    else:
        print(f"File {file_path} does not exist. Transformation skipped.")

with DAG(
    'download_transform_arxiv_data',
    default_args=default_args,
    description='DAG to download, transform and save arxiv dataset',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='download_dataset',
        python_callable=download_dataset,
    )

    t2 = PythonOperator(
        task_id='unzip_dataset',
        python_callable=unzip_dataset,
    )

    t3 = PythonOperator(
        task_id='transform_and_save',
        python_callable=transform_and_save,
    )

    t1 >> t2 >> t3
