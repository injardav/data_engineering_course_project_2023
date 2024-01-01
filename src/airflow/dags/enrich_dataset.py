from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from utils.api import consume_semantic_scholar

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('enrich_arxiv_data',
         default_args=default_args,
         description='DAG to enrich arxiv dataset with Semantic Scholar API data',
         schedule_interval=None,  # Manually triggered or triggered by sensor
         catchup=False) as dag:

    wait_for_clean_and_validate = ExternalTaskSensor(
        task_id='wait_for_clean_and_validate',
        external_dag_id='clean_and_validate_arxiv_data',
        external_task_id='clean_and_validate_dataset',  # Waiting for this task to complete
        timeout=600,
        poke_interval=30
    )

    t4 = PythonOperator(task_id='enrich_dataset',
                        python_callable=consume_semantic_scholar,
                        provide_context=True)

    wait_for_clean_and_validate >> t4
