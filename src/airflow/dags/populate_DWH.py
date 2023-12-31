from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import os
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    'populate_DWH',
    default_args=default_args,
    description='This fact for populating the DataWearHouse',
    schedule_interval=timedelta(days=1)
)

# Define a function to insert data into each table
def insert_data(table_name, file_path, sql_statement, **kwargs):
    # Read data from the specified file
    with open(file_path, 'r') as f:
        data = json.load(f)
    print(f'file_data: {data}')
    # Use the data to construct your SQL
    sql = sql_statement.format(file_data=data)

    print(f'SQL Statement: {sql}')
    #invalid_keys = ['db_conn_type', 'db_host', 'user_login', 'user_password', 'db_port', 'dwh_database']
    #valid_kwargs = {k: v for k, v in kwargs.items() if k not in invalid_keys}
    # Execute the SQL using PostgresOperator
    insert_task = PostgresOperator(
        task_id=f'insert_task_{table_name}',
        sql=sql,
        postgres_conn_id='postgres_default',  # Set to None to bypass using postgres_conn_id
        dag=kwargs.get('dag'),  # Add the 'dag' key explicitly
        params={
            'postgres_conn_type':'postgres',  # Database connection type
            'postgres_conn_host':'postgres',
            'postgres_conn_user':'project_user',
            'postgres_conn_password':'project_pass123',
            'postgres_conn_port':5432,  # Your Postgres port
            'postgres_conn_database':'project_dwh',
            }
        #dag=dag
    )

# Define the tables along with their file paths and SQL statements
tables = {
    'publication_fact': {
        'file_path': '/opt/airflow/dags/publication_fact.json',
        'sql_statement': """
            INSERT INTO publication_fact (reference_count, score, doi, start_date)
            VALUES ({file_data[reference_count]}, {file_data[score]}, '{file_data[doi]}', '{file_data[start_date]}');
        """,
    },
    'dim_author_affiliation': {
        'file_path': '/opt/airflow/dags/dim_author_affiliation.json',
        'sql_statement': """
            INSERT INTO dim_author_affiliation (affiliation, is_current, start_date, end_date)
            VALUES ('{file_data[affiliation]}', {file_data[is_current]}, '{file_data[start_date]}', '{file_data[end_date]}');
        """,
    },
    'dim_authors': {
        'file_path': '/opt/airflow/dags/dim_authors.json',
        'sql_statement': """
            INSERT INTO dim_authors (aff_id, first_name, family_name)
            VALUES ({file_data[aff_id]}, '{file_data[first_name]}', '{file_data[family_name]}');
        """,
    },
    'dim_publication': {
        'file_path': '/opt/airflow/dags/dim_publication.json',
        'sql_statement': """
            INSERT INTO dim_publication (
                pid, id, submitter, article_number, title, journal_ref, general_category,
                type, issue, language, short_container_title, container_title,
                is_referenced_by_count, is_current, start_date, end_date
            ) VALUES (
                {file_data[pid]}, {file_data[id]}, '{file_data[submitter]}',
                '{file_data[article_number]}', '{file_data[title]}', '{file_data[journal_ref]}',
                '{file_data[general_category]}', '{file_data[type]}', '{file_data[issue]}',
                '{file_data[language]}', '{file_data[short_container_title]}',
                '{file_data[container_title]}', {file_data[is_referenced_by_count]},
                {file_data[is_current]}, '{file_data[start_date]}', '{file_data[end_date]}'
            );
        """,
    },
    'dim_publish_sn': {
        'file_path': '/opt/airflow/dags/dim_publish_sn.json',
        'sql_statement': """
            INSERT INTO dim_publish_sn (pid, issn_number, issn_type)
            VALUES ({file_data[pid]}, '{file_data[issn_number]}', '{file_data[issn_type]}');
        """,
    },
    'dim_license': {
        'file_path': '/opt/airflow/dags/dim_license.json',
        'sql_statement': """
            INSERT INTO dim_license (license_start, license_url, license_content_version, license_delay)
            VALUES ('{file_data[license_start]}', '{file_data[license_url]}',
                '{file_data[license_content_version]}', {file_data[license_delay]});
        """,
    },
    'dim_publisher': {
        'file_path': '/opt/airflow/dags/dim_publisher.json',
        'sql_statement': """
            INSERT INTO dim_publisher (pub_id, publisher_name)
            VALUES ({file_data[pub_id]}, '{file_data[publisher_name]}');
        """,
    },
    'dim_references': {
        'file_path': '/opt/airflow/dags/dim_references.json',
        'sql_statement': """
            INSERT INTO dim_references (pid, doi, key, doi_asserted_by)
            VALUES ({file_data[pid]}, '{file_data[doi]}', '{file_data[key]}',
                '{file_data[doi_asserted_by]}');
        """,
    },
    'dim_pub_version': {
        'file_path': '/opt/airflow/dags/dim_pub_version.json',
        'sql_statement': """
            INSERT INTO dim_pub_version (pid, vid, created_time)
            VALUES ({file_data[pid]}, '{file_data[vid]}', '{file_data[created_time]}');
        """,
    },
}

# Create tasks for each table
tasks={}
for table, config in tables.items():
    insert_data_task = PythonOperator(
        task_id=f'insert_data_task_{table}',
        python_callable=insert_data,
        provide_context=True,
        op_args=[table, config['file_path'], config['sql_statement']],
        dag=dag
    )
    tasks[table] = insert_data_task
tasks['publication_fact'] >> tasks['dim_author_affiliation'] >> tasks['dim_authors'] >> tasks['dim_publication'] >> tasks['dim_publish_sn'] >> tasks['dim_license'] >> tasks['dim_publisher'] >> tasks['dim_references'] >> tasks['dim_pub_version']
