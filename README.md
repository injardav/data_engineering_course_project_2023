# Data Engineering (LTAT.02.007) Project 
Designing and Implementing a Data Pipeline to Analyze Scientific Publications.

## Local Ports
- Airflow `http://localhost:8080/`
- Postgres `http://localhost:5432/`
- Neo4j `http://localhost:7474/`

## Usage Instructions
- Make sure you have installed Docker Desktop (Windows).
- If you are using Windows, consider using Git Bash or some other Unix type terminal.

### Initial Setup
During the initial setup we need to initialize Airflow before we can run it.

1. `docker-compose up -d postgres neo4j`
2. `docker-compose run --rm airflow-webserver db init`
3. `docker-compose up -d`

The initialization persists across container restarts as long as the data in your PostgreSQL and Neo4j volumes is not deleted. This means you won't need to re-run db init each time you start your containers unless the data volumes are cleared or you are starting fresh.

Create an Admin user:

4. `docker-compose run --rm airflow-webserver bash`
5. `airflow users create --username admin --firstname Admin --lastname Admin --role Admin --email admin@example.com --password admin`

### Normal Setup
`docker-compose up -d`
