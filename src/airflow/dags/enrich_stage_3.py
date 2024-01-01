from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
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

    enrich = PythonOperator(task_id='enrich_dataset',
                        python_callable=consume_semantic_scholar,
                        op_args=[base_file_path, base_file_destination],
                        provide_context=True)
    
    run_insert_into_neo4j = TriggerDagRunOperator(
        task_id='trigger_insert_into_neo4j',
        trigger_dag_id='insert_into_neo4j_stage_4',
    )

    run_insert_into_postgres = TriggerDagRunOperator(
        task_id='trigger_insert_into_postgres',
        trigger_dag_id='insert_into_postgres_stage_4',
    )

    enrich >> [run_insert_into_neo4j, run_insert_into_postgres]
