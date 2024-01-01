from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from utils.utils import clean_and_validate_dataset

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('clean_and_validate_arxiv_data',
         default_args=default_args,
         description='DAG to preprocess arxiv dataset',
         schedule_interval=None,  # Manually triggered or triggered by sensor
         catchup=False) as dag:

    wait_for_download_and_unzip = ExternalTaskSensor(
        task_id='wait_for_download_and_unzip',
        external_dag_id='download_and_unzip_arxiv_data',
        external_task_id='unzip_dataset',  # Waiting for this task to complete
        timeout=600,
        poke_interval=30
    )

    t3 = PythonOperator(task_id='clean_and_validate_dataset',
                        python_callable=clean_and_validate_dataset,
                        provide_context=True)

    wait_for_download_and_unzip >> t3
