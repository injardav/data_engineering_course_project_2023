import os, json, zipfile, pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
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

def get_unique_categories(row):
    return ' '.join(sorted(set(row.split())))

def load_category_mapping(file_path):
    logger.info("Loading category mapping json")
    with open(file_path, 'r') as file:
        return json.load(file)
    
def map_category(row, mapping):
    categories = row.split()
    return ' '.join(mapping.get(cat, cat) for cat in categories)

def download_dataset():
    dataset_path = '/opt/airflow/dataset/arxiv.zip'

    if not os.path.exists(dataset_path):
        try:
            logger.info("Dataset did not exist, attempting to download")
            api = KaggleApi()
            api.authenticate()
            api.dataset_download_files('Cornell-University/arxiv', path='/opt/airflow/dataset/', unzip=False)
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
    output_path = '/opt/airflow/staging_area/arxiv_transformed.csv'

    if os.path.exists(file_path):
        df = pd.read_json(file_path, lines=True)
        df = df.dropna(subset=['doi'])
        df.reset_index(drop=True, inplace=True)
        df.index += 1
        df['id'] = df.index

        # General Category mapping
        logger.info("Starting general category mapping")
        logger.info("First 10 unique 'categories' values:\n" + str(df['categories'].unique()[:10]))

        df['categories'] = df['categories'].apply(get_unique_categories)
        category_mapping = load_category_mapping('/opt/airflow/data/category_mapping.json')
        df['general_category'] = df['categories'].apply(lambda x: map_category(x, category_mapping))
        df.drop('categories', axis=1, inplace=True)

        logger.info("First 10 'general_category' values:\n" + str(df['general_category'].unique()[:10]))

        # Save the DataFrame to CSV
        df.to_csv(output_path, index=False)
        logger.info(f"DataFrame saved to {output_path}")
    else:
        logger.info(f"File {file_path} does not exist. Transformation and save operation skipped.")

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
        task_id='transform_and_save_dataframe',
        python_callable=transform_and_save_dataframe,
    )

    t1 >> t2 >> t3
