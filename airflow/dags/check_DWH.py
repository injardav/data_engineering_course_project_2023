from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2

# Connection parameters
db_conn = {
    'host': 'postgres',
    'port': 5432,
    'user': 'project_user',
    'password': 'project_pass123',
    'database': 'project_dwh',
}

# DWH Schema
table_creation_queries = {
    'publication_fact': """
        CREATE TABLE publication_fact (
            id SERIAL PRIMARY KEY,
            reference_count INTEGER,
            score INTEGER,
            doi VARCHAR(255),
            start_date TIMESTAMP
        );
    """,
	'dim_author_affiliation': """
        CREATE TABLE dim_author_affiliation (
            aff_id SERIAL PRIMARY KEY,
            affiliation VARCHAR(255),
            is_current BOOLEAN,
            start_date TIMESTAMP,
            end_date TIMESTAMP
        );
    """,

    'dim_authors': """
        CREATE TABLE dim_authors (
            aid SERIAL PRIMARY KEY,
            aff_id INTEGER REFERENCES dim_author_affiliation(aff_id),
            first_name VARCHAR(255),
            family_name VARCHAR(255)
        );
    """,

    'dim_publication': """
        CREATE TABLE dim_publication (
            pid SERIAL PRIMARY KEY,
			id INTEGER REFERENCES publication_fact(id),
            submitter VARCHAR(255),
            article_number VARCHAR(255),
            title VARCHAR(255),
            journal_ref VARCHAR(255),
            general_category VARCHAR(255),
            type VARCHAR(255),
            issue VARCHAR(255),
            language VARCHAR(255),
            short_container_title VARCHAR(255),
            container_title VARCHAR(255),
            is_referenced_by_count INTEGER,
            is_current BOOLEAN,
            start_date TIMESTAMP,
            end_date TIMESTAMP
        );
    """,

    'dim_publish_sn': """
        CREATE TABLE dim_publish_sn (
            pid INTEGER REFERENCES dim_publication(pid),
            issn_number VARCHAR(255),
            issn_type VARCHAR(255),
            PRIMARY KEY (pid, issn_number)
        );
    """,

    'dim_license': """
        CREATE TABLE dim_license (
            lid SERIAL PRIMARY KEY,
            license_start TIMESTAMP,
            license_url VARCHAR(255),
            license_content_version VARCHAR(255),
            license_delay INTEGER
        );
    """,

    'dim_publisher': """
        CREATE TABLE dim_publisher (
            pub_id SERIAL PRIMARY KEY,
            publisher_name VARCHAR(255)
        );
    """,

    'dim_references': """
        CREATE TABLE dim_references (
            pid INTEGER REFERENCES dim_publication(pid),
            doi VARCHAR(255),
            key VARCHAR(255),
            doi_asserted_by VARCHAR(255),
            PRIMARY KEY (pid, doi)
        );
    """,

    'dim_pub_version': """
        CREATE TABLE dim_pub_version (
            pid INTEGER REFERENCES dim_publication(pid),
            vid VARCHAR(255),
            created_time TIMESTAMP,
            PRIMARY KEY (pid, vid)
        );
    """
}

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
    'create_dwh_tables',
    default_args=default_args,
    description='DAG to create Data Warehouse schema and tables',
    schedule_interval='@once',  # Set the frequency of DAG runs
    is_paused_upon_creation=False,
)

def execute_query(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
    #    result = cursor.fetchall()
    #return result


def check_table_and_create(conn, table_name, create_table_query):
    # Check if the table exists
    check_table_query = f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}';"
    with conn.cursor() as cursor:
        cursor.execute(check_table_query)
        table_exists = cursor.fetchone() is not None

    # If the table does not exist, create it
    if not table_exists:
        execute_query(conn, create_table_query)
        print(f"Table '{table_name}' created ")

def create_dwh_tables(**kwargs):
    # Connect to PostgreSQL server
    conn_params = {
        'host': db_conn['host'],
        'port': db_conn['port'],
        'user': db_conn['user'],
        'password': db_conn['password'],
        'database': db_conn['database'],
    }

    conn = psycopg2.connect(**conn_params)

    # Check and create schema
    #check_schema_and_create(conn, db_conn['schema'])

    # Check and create tables
    for table_name, create_table_query in table_creation_queries.items():
        check_table_and_create(conn,  table_name, create_table_query)
    conn.commit()
    # Close the database connection
    conn.close()

# Define the PythonOperator to run the create_dwh_schema_and_tables function
create_dwh_task = PythonOperator(
    task_id='create_dwh_task',
    python_callable=create_dwh_tables,
    provide_context=True,  # Pass the task context (including connection parameters)
    dag=dag,
)

# Set the task dependencies
create_dwh_task
