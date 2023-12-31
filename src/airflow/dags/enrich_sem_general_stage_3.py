from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from utils.api import semantic_general

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
base_file_destination = '/opt/airflow/staging_area/arxiv_enriched_sem_general_part_'

with DAG('enrich_sem_general_stage_3',
         default_args=default_args,
         description='Enrich dataset with general data from Semantic Scholar API (bulk papers data)',
         schedule_interval=None,  # Manually triggered or triggered by sensor
         catchup=False) as dag:

    enrich_sem_general = PythonOperator(task_id='enrich_dataset_sem_general',
                        python_callable=semantic_general,
                        op_args=[base_file_path, base_file_destination],
                        provide_context=True)
    
    insert_sem_general_neo4j = TriggerDagRunOperator(
        task_id='insert_sem_general_neo4j',
        trigger_dag_id='insert_sem_general_neo4j_stage_4',
    )

    insert_sem_general_postgres = TriggerDagRunOperator(
        task_id='insert_sem_general_postgres',
        trigger_dag_id='insert_sem_general_postgres_stage_4',
    )

    enrich_sem_additional = TriggerDagRunOperator(
        task_id='enrich_sem_general',
        trigger_dag_id='enrich_sem_additional_stage_5',
    )

    enrich_sem_general >> [insert_sem_general_neo4j, insert_sem_general_postgres] >> enrich_sem_additional
