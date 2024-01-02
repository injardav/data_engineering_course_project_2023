from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils.databases import insert_into_postgres

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('insert_into_postgres_stage_4',
         default_args=default_args,
         description='DAG to insert preprocessed and enriched data into postgres DWH',
         schedule_interval=None,  # Manually triggered or triggered by sensor
         catchup=False) as dag:

    insert_into_postgres = PythonOperator(task_id='insert_into_postgres',
                        python_callable=insert_into_postgres,
                        provide_context=True)

    insert_into_postgres
