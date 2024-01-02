from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils.databases import insert_into_neo4j

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

queries_directory = '/opt/airflow/neo4j/queries'
base_input_path = '/opt/airflow/staging_area/arxiv_enriched_part_'

with DAG('insert_into_neo4j_stage_4',
         default_args=default_args,
         description='DAG to insert preprocessed and enriched data into Neo4j graph database',
         schedule_interval=None,  # Manually triggered or triggered by sensor
         catchup=False) as dag:

    insert_into_neo4j = PythonOperator(task_id='insert_into_neo4j',
                        python_callable=insert_into_neo4j,
                        op_args=[queries_directory, base_input_path],
                        provide_context=True)

    insert_into_neo4j
