import os, zipfile
from datetime import datetime, timedelta
from airflow import DAG
from utils.utils import *
from utils.api import consume_crossref, consume_semantic_scholar
from utils.databases import insert_into_neo4j
from airflow.operators.python_operator import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
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

logger = LoggingMixin().log

def download_dataset():
    dataset_path = '/opt/airflow/dataset/arxiv.zip'

    if not os.path.exists(dataset_path):
        try:
            logger.info("Dataset did not exist, attempting to download")
            kaggle_api = KaggleApi()
            kaggle_api.authenticate()
            kaggle_api.dataset_download_files('Cornell-University/arxiv', path='/opt/airflow/dataset/', unzip=False)
        except Exception as e:
            logger.error(f"Failed to download dataset: {e}")
            raise

def unzip_dataset():
    dataset_path = '/opt/airflow/dataset/arxiv.zip'
    extracted_path = '/opt/airflow/dataset/arxiv-metadata-oai-snapshot.json'

    if os.path.exists(dataset_path) and not os.path.exists(extracted_path):
        try:
            with zipfile.ZipFile(dataset_path, 'r') as zip_ref:
                zip_ref.extractall('/opt/airflow/dataset/')
        except zipfile.BadZipFile as e:
            logger.error(f"Failed to unzip dataset: {e}")
            raise

def transform_and_save_dataframe():
    file_path = '/opt/airflow/dataset/arxiv-metadata-oai-snapshot.json'
    base_output_path = '/opt/airflow/staging_area/arxiv_transformed_part_'
    
    if not os.path.exists(file_path):
        logger.info(f"File {file_path} does not exist. Operation skipped.")
        return

    # total_rows = get_total_rows(file_path)
    total_rows = 500
    rows_per_subset = total_rows // 4

    for part in range(4):
        # Load a subset of the dataset
        subset_start_row = part * rows_per_subset
        df = load_dataset(file_path, subset=True, start_row=subset_start_row, rows=rows_per_subset)
        
        # Process the DataFrame
        handle_id(df)
        handle_authors(df)
        map_general_categories(df, logger)
        consume_crossref(df, logger)
        # consume_semantic_scholar(df, logger)

        # Save the processed subset
        output_path = f"{base_output_path}{part}.json"
        df.to_json(output_path, orient='records', lines=True)
        logger.info(f"Subset {part} of DataFrame saved to {output_path}")

with DAG('download_transform_arxiv_data', default_args=default_args, description='DAG to download, transform and save arxiv dataset', schedule_interval=timedelta(days=1), catchup=False) as dag:
    t1 = PythonOperator(task_id='download_dataset', python_callable=download_dataset, provide_context=True)
    t2 = PythonOperator(task_id='unzip_dataset', python_callable=unzip_dataset, provide_context=True)
    t3 = PythonOperator(task_id='transform_and_save_dataframe', python_callable=transform_and_save_dataframe, provide_context=True)
    t4 = PythonOperator(task_id='insert_into_neo4j', python_callable=insert_into_neo4j, provide_context=True)

    t1 >> t2 >> t3 >> t4
