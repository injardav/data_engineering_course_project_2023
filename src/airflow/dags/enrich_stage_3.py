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

base_file_path = '/opt/airflow/staging_area/arxiv_preprocessed_part_'
base_file_destination = '/opt/airflow/staging_area/arxiv_enriched_part_'

with DAG('enrich_stage_3',
         default_args=default_args,
         description='DAG to enrich arxiv dataset with Semantic Scholar API data',
         schedule_interval=None,  # Manually triggered or triggered by sensor
         catchup=False) as dag:

    wait_for_clean_and_validate = ExternalTaskSensor(
        task_id='wait_for_clean_and_validate',
        external_dag_id='clean_and_validate_stage_2',
        external_task_id='delete_json_file',  # Waiting for this task to complete
        timeout=60 * 60 * 24 * 8,  # 1 week,
        poke_interval=30
    )

    enrich = PythonOperator(task_id='enrich_dataset',
                        python_callable=consume_semantic_scholar,
                        op_args=[base_file_path, base_file_destination],
                        provide_context=True)

    wait_for_clean_and_validate >> enrich
