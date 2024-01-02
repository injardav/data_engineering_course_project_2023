from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
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

    run_sem_general_enrichment = TriggerDagRunOperator(
        task_id='trigger_sem_general_enrichment',
        trigger_dag_id='enrich_sem_general_stage_3',
    )

    clean_and_validate >> delete >> run_sem_general_enrichment
