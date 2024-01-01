from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils.utils import download_dataset, unzip_dataset

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('download_and_unzip_arxiv_data',
         default_args=default_args,
         description='DAG to download and unzip arxiv dataset',
         schedule_interval='0 0 * * 0',  # Weekly at midnight on Sunday
         catchup=False) as dag:

    t1 = PythonOperator(task_id='download_dataset',
                        python_callable=download_dataset,
                        provide_context=True)

    t2 = PythonOperator(task_id='unzip_dataset',
                        python_callable=unzip_dataset,
                        provide_context=True)

    t1 >> t2
