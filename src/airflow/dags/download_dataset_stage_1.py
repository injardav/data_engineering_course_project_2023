from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from utils.utils import download_dataset, unzip_dataset, delete_file

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

kaggle_dataset = 'Cornell-University/arxiv'
dataset_path = '/opt/airflow/dataset/arxiv.zip'
extracted_path = '/opt/airflow/dataset/arxiv-metadata-oai-snapshot.json'

with DAG('download_dataset_stage_1',
         default_args=default_args,
         description='DAG to download and unzip arXiv dataset',
         schedule_interval='0 0 * * 0',  # Weekly at midnight on Sunday
         catchup=False) as dag:

    download = PythonOperator(
        task_id='download_dataset',
        python_callable=download_dataset,
        op_args=[kaggle_dataset, dataset_path],
        provide_context=True
    )

    unzip = PythonOperator(
        task_id='unzip_dataset',
        python_callable=unzip_dataset,
        op_args=[dataset_path, extracted_path],
        provide_context=True
    )
    
    delete = PythonOperator(
        task_id='delete_zip_file',
        python_callable=delete_file,
        op_args=[dataset_path],
        provide_context=True
    )

    run_clean_and_validate = TriggerDagRunOperator(
        task_id='trigger_clean_and_validate',
        trigger_dag_id='clean_and_validate_stage_2',
    )

    download >> unzip >> delete >> run_clean_and_validate
