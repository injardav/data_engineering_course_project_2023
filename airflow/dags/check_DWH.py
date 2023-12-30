from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import psycopg2

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'check_dwh_implementation',
    default_args=default_args,
    description='DAG to check Data Warehouse implementation',
    schedule_interval='@daily',  # Set the frequency of DAG runs
)

# Define a Python function to check DWH implementation
def check_dwh_implementation(**kwargs):
    # Get the connection parameters from Airflow connection
    conn_id = kwargs.get('conn_id', 'your_dwh_connection_id')
    db_conn = BaseHook.get_connection(conn_id)

    # Establish a connection to the DWH
    conn = psycopg2.connect(
        host=db_conn.host,
        port=db_conn.port,
        user=db_conn.login,
        password=db_conn.password,
        database=db_conn.schema,
    )

    # List of fact and dimension tables
    tables_to_check = [
        'Publication_Fact',
        'Dim_Publication',
        'Dim_authors',
        'Dim_author_affiliation',
        'Dim_publisher',
        'Dim_Publish_SN',
        'Dim_license',
        'Dim_Refreneces',
        'Dim_pub_version',
    ]

    # Check each table
    for table in tables_to_check:
        check_table_query = f'SELECT COUNT(*) FROM {table};'
        with conn.cursor() as cursor:
            cursor.execute(check_table_query)
            result = cursor.fetchone()[0]

        print(f'Checking {table}: {result} rows found.')

    # Close the database connection
    conn.close()

# Define the PythonOperator to run the check_dwh_implementation function
check_dwh_task = PythonOperator(
    task_id='check_dwh_task',
    python_callable=check_dwh_implementation,
    provide_context=True,  # Pass the task context (including connection parameters)
    dag=dag,
)

# Set the task dependencies
check_dwh_task

if __name__ == "__main__":
    dag.cli()

