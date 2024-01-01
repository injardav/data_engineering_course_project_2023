from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from utils.utils import clean_and_validate_dataset, delete_file

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

file_path = '/opt/airflow/dataset/arxiv-metadata-oai-snapshot.json'

with DAG('clean_and_validate_stage_2',
         default_args=default_args,
         description='DAG to preprocess arxiv dataset',
         schedule_interval=None,  # Manually triggered or triggered by sensor
         catchup=False) as dag:

    wait_for_download_and_unzip = ExternalTaskSensor(
        task_id='wait_for_download_and_unzip',
        external_dag_id='download_and_unzip_arxiv_data',
        external_task_id='delete_zip_file',  # Waiting for this task to complete
        timeout=60 * 60 * 24 * 8,  # 1 week and 1 day (because new data is downloaded weekly, we add some buffer time)
        poke_interval=30
    )

    clean_and_validate = PythonOperator(
        task_id='clean_and_validate_dataset',
        python_callable=clean_and_validate_dataset,
        op_args=[file_path],
        provide_context=True
    )
    
    delete = PythonOperator(
        task_id='delete_json_file',
        python_callable=delete_file,
        op_args=[file_path],
        provide_context=True
    )

    wait_for_download_and_unzip >> clean_and_validate
