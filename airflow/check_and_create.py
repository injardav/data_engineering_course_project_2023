import psycopg2
from neo4j import GraphDatabase

def check_postgresql_dwh(host, port, username, password, database, schema):
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database
        )
        cursor = connection.cursor()

        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s", (schema,))
        schema_exists = cursor.fetchone() is not None

        cursor.close()
        connection.close()

        return schema_exists

    except Exception as e:
        print(f"Error checking PostgreSQL DWH: {e}")
        return False

def create_postgresql_dwh(host, port, username, password, database, schema, sql_script):
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database
        )
        cursor = connection.cursor()

        # Create schema if it doesn't exist
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        connection.commit()

        # Set search path to the new schema
        cursor.execute(f"SET search_path TO {schema};")
        connection.commit()

        # Execute SQL script
        with open(sql_script, 'r') as file:
            sql_statements = file.read()
            cursor.execute(sql_statements)
            connection.commit()

        cursor.close()
        connection.close()

        print(f"PostgreSQL DWH Schema '{schema}' created successfully.")

    except Exception as e:
        print(f"Error creating PostgreSQL DWH: {e}")

def check_neo4j_database(uri, username, password, database):
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        session = driver.session()

        result = session.run(f"SHOW DATABASES LIKE '{database}'")
        database_exists = bool(result.single())

        session.close()
        driver.close()

        return database_exists

    except Exception as e:
        print(f"Error checking Neo4j database: {e}")
        return False

def create_neo4j_database(uri, username, password, database, cql_script):
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        session = driver.session()

        # Create database if it doesn't exist
        session.run(f"CREATE DATABASE IF NOT EXISTS {database}")
        session.close()

        # Use the new database
        uri = f"bolt://{uri}/{database}"
        driver = GraphDatabase.driver(uri, auth=(username, password))
        session = driver.session()

        # Execute CQL script
        with open(cql_script, 'r') as file:
            cql_statements = file.read()
            session.run(cql_statements)

        session.close()
        driver.close()

        print(f"Neo4j Database '{database}' created successfully.")

    except Exception as e:
        print(f"Error creating Neo4j database: {e}")

# Replace these values with your actual database and schema details
postgresql_host = 'your_postgresql_host'
postgresql_port = 'your_postgresql_port'
postgresql_username = 'your_postgresql_username'
postgresql_password = 'your_postgresql_password'
postgresql_database = 'your_postgresql_database'
postgresql_schema = 'your_postgresql_schema'
postgresql_sql_script = 'path/to/your_postgresql_script.sql'

neo4j_uri = 'your_neo4j_host:7687'
neo4j_username = 'your_neo4j_username'
neo4j_password = 'your_neo4j_password'
neo4j_database = 'your_neo4j_database'
neo4j_cql_script = 'path/to/your_neo4j_script.cql'

# Check PostgreSQL DWH schema existence
postgresql_schema_exists = check_postgresql_dwh(postgresql_host, postgresql_port, postgresql_username,
                                               postgresql_password, postgresql_database, postgresql_schema)

# Check Neo4j database existence
neo4j_database_exists = check_neo4j_database(neo4j_uri, neo4j_username, neo4j_password, neo4j_database)

# Create PostgreSQL DWH schema if not exists
if not postgresql_schema_exists:
    create_postgresql_dwh(postgresql_host, postgresql_port, postgresql_username,
                           postgresql_password, postg
