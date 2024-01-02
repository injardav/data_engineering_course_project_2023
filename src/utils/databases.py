import re, os, pandas as pd
from datetime import datetime
from neo4j import GraphDatabase

class Neo4jConnector:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def execute_query(self, query, parameters=None):
        with self._driver.session() as session:
            result = session.run(query, parameters)
            return result
        
    def create_index(neo4j_connector, label, property):
        # Check if the index already exists
        index_exists_query = f"SHOW INDEXES YIELD labelsOrTypes, properties WHERE '{label}' IN labelsOrTypes AND '{property}' IN properties RETURN COUNT(*)"
        with neo4j_connector._driver.session() as session:
            result = session.run(index_exists_query)
            index_exists = result.single()[0] > 0

        if not index_exists:
            # Create the index if it does not exist
            create_index_query = f"CREATE INDEX FOR (n:{label}) ON (n.{property})"
            with neo4j_connector._driver.session() as session:
                session.run(create_index_query)

    def execute_file(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            query = file.read()
            self.execute_query(query)

def load_and_execute_queries(directory, neo4j_connector, logger):
    for filename in os.listdir(directory):
        if filename.endswith('.cql') or filename.endswith('.cypher'):
            file_path = os.path.join(directory, filename)
            logger.info(f"Executing query from file: {filename}")
            neo4j_connector.execute_file(file_path)

def process_sem_general(df_batch, neo4j_connector, logger):
    logger.info(f"Starting to process batch of length {len(df_batch)}")
    batch_data = df_batch.to_dict('records')
    author_uuids = {}
    authors_data = []
    references_data = []
    versions_data = []

    def convert_to_iso_format(date_str):
        # Assuming the date is in a format like "Mon, 2 Apr 2007 19:18:42 GMT"
        # Adjust the format string according to your actual date format
        try:
            date_obj = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S GMT')
            return date_obj.isoformat()
        except ValueError:
            # Handle the error or return the original string if conversion fails
            return date_str
        
    def clean_text(text):
        # Remove newline characters
        text = text.replace('\n', ' ').replace('\r', ' ')

        # Replace multiple spaces with a single space
        text = re.sub(r'\s+', ' ', text)

        text = text.strip()
        return text

    for row_index, row in enumerate(batch_data):
        try:
            # Update names
            for author in row.get('authors_parsed', []):
                row['author']['first_name'] = author[0]
                row['author']['last_name'] = author[1]

            # Convert date to a standard format
            if row.get('update_date'):
                row['update_date'] = convert_to_iso_format(row['update_date'])
            
            # Clean up text fields
            for field in ['comments']:
                if row.get(field):
                    row[field] = clean_text(row[field])

            # Handle null values
            for field in ['license', 'journal_ref', 'doi']:
                if field not in row or row[field] is None:
                    row[field] = 'Unknown'  # or some other default value


        except Exception as e:
            logger.error(f"Error processing row index {row_index} with ID {row.get('id', 'Unknown ID')}: {e}")
            logger.error(f"Problematic row data: {row}")

    try:
        versions_query = """
            UNWIND $versions_data AS ver        
            CREATE (:Version {
                id: ver.id,
                version_id: ver.version_id,
                created_time: ver.created_time,
                version: ver.version
            })
        """
        logger.info("Executing versions query")
        neo4j_connector.execute_query(versions_query, {'versions_data': versions_data})

        authors_query = """
            UNWIND $authors_data AS author
            CREATE (:Author {
                author_id: author.author_id,
                id: author.id,
                name: author.name
            })
        """
        logger.info("Executing authors query")
        neo4j_connector.execute_query(authors_query, {'authors_data': authors_data})

        references_query = """
            UNWIND $references_data AS ref
            CREATE(:ReferencedPublication {
                id: ref.id,
                ref_doi: ref.ref_doi,
                ref_key: ref.ref_key,
                ref_doi_asserted_by: ref.ref_doi_asserted_by
            })
        """
        logger.info("Executing references query")
        neo4j_connector.execute_query(references_query, {'references_data': references_data})

        publications_query = """
            UNWIND $batch AS row
            CREATE (:Paper {
                id: row.id,
                arxiv: row.arxiv,
                comments: row.comments,
                journal_ref: row.comments,
                doi: row.doi,
                report_no: row.report_no,
                license: row.license,
                update_date: row.update_date,
                general_category: row.general_category
            })
        """
        logger.info("Executing publications query")
        neo4j_connector.execute_query(publications_query, {'batch': batch_data})

        journal_query = """
            UNWIND $batch AS row
            CREATE (:Journal {
                id: row.id,
                name: row.journal_name,
                pages: row.journal_pages,
                volume: row.journal_volume,
                start_date: null,
                end_date: null
            })
        """
        logger.info("Executing journal query")
        neo4j_connector.execute_query(journal_query, {'batch': batch_data})

        auth_affiliations_query = """
            UNWIND $batch AS row
            CREATE (:Affiliation {
                id: row.id,
                affiliation: row.affiliation,
                is_current: row.is_current,
                start_date: null,
                end_date: null
            })
        """
        logger.info("Executing authors affiliation query")
        neo4j_connector.execute_query(auth_affiliations_query, {'batch': batch_data})

        publishers_query = """
            UNWIND $batch AS row
            CREATE (:Publisher {
                id: row.id,
                publisher_name: row.publisher
            })
        """
        logger.info("Executing publishers query")
        neo4j_connector.execute_query(publishers_query, {'batch': batch_data})

        PublisherSerialNumber_query = """
            UNWIND $batch AS row
            CREATE (:PublisherSerialNumber {
                id: row.id,
                issn_values: row.ISSN_type_values,
                issn_types: row.ISSN_type_types
            })
        """
        logger.info("Executing publishers issn query")
        neo4j_connector.execute_query(PublisherSerialNumber_query, {'batch': batch_data})

        licenses_query = """
            UNWIND $batch AS row
            CREATE (:License {
                id: row.id,
                license_start: row.license_start,
                license_url: row.license_url,
                license_content_version: row.license_content_version,
                license_delay: row.license_delay
            })
        """
        logger.info("Executing license query")
        neo4j_connector.execute_query(licenses_query, {'batch': batch_data})

        facts_query = """
            UNWIND $batch AS row
            CREATE (:PublicationMetrics {
                id: row.id,
                references_count: row.references_count,
                score: row.score,
                doi: row.doi,
                start_date: null
            })
        """
        logger.info("Executing facts query")
        neo4j_connector.execute_query(facts_query, {'batch': batch_data})

        relationships_query_1 = """
            UNWIND $batch AS row
            MATCH (pub:Paper {id: row.id})
            MATCH (auth:Author {id: row.id})
            CREATE (pub)-[:AUTHORED_BY]->(auth)
        """
        logger.info("Executing relationships query 1")
        neo4j_connector.execute_query(relationships_query_1, {'batch': batch_data})

        relationships_query_2 = """
            UNWIND $batch AS row
            MATCH (auth:Author {id: row.id})
            MATCH (aff:Affiliation {id: row.id})
            CREATE (auth)-[:AFFILIATED_WITH]->(aff)
        """
        logger.info("Executing relationships query 2")
        neo4j_connector.execute_query(relationships_query_2, {'batch': batch_data})

        relationships_query_3 = """
            UNWIND $batch AS row
            MATCH (serial:PublisherSerialNumber {id: row.id})
            CREATE (pub)-[:HAS_ISSN]->(serial)
        """
        logger.info("Executing relationships query 3")
        neo4j_connector.execute_query(relationships_query_3, {'batch': batch_data})

        relationships_query_4 = """
            UNWIND $batch AS row
            MATCH (lic:License {id: row.id})
            CREATE (pub)-[:HAS_LICENSE]->(lic)
        """
        logger.info("Executing relationships query 4")
        neo4j_connector.execute_query(relationships_query_4, {'batch': batch_data})

        relationships_query_5 = """
            UNWIND $batch AS row
            MATCH (ref:ReferencedPublication {id: row.id})
            CREATE (pub)-[:HAS_REFERENCE]->(ref)
        """
        logger.info("Executing relationships query 5")
        neo4j_connector.execute_query(relationships_query_5, {'batch': batch_data})
        
        relationships_query_6 = """
            UNWIND $batch AS row
            MATCH (ver:Version {id: row.id})
            CREATE (pub)-[:HAS_VERSION]->(ver)
        """
        logger.info("Executing relationships query 6")
        neo4j_connector.execute_query(relationships_query_6, {'batch': batch_data})

        relationships_query_7 = """
            UNWIND $batch AS row
            MATCH (fact:PublicationMetrics {doi: row.doi})
            MATCH (pub:Paper {doi: row.doi})
            CREATE (fact)-[:BASED_ON_PUBLICATION]->(pub)
        """
        logger.info("Executing relationships query 7")
        neo4j_connector.execute_query(relationships_query_7, {'batch': batch_data})

        logger.info("Successfully processed and inserted batch data into Neo4j")

    except Exception as e:
        logger.error(f"Error executing Neo4j queries: {e}")

def process_sem_additional(df_batch, neo4j_connector, logger):
    ...

def insert_sem_general_neo4j(queries_directory, base_input_path, batch_size=500, **kwargs):
    NEO4J_URI = "bolt://neo4j:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "project_pass123"
    part_count = 4

    indexes_to_create = [
        ("Paper", "id"),
        ("Author", "id"),
        ("Affiliation", "id"),
        ("PublisherSerialNumber", "id"),
        ("License", "id"),
        ("ReferencedPublication", "id"),
        ("Version", "id"),
        ("PublicationMetrics", "doi")
    ]

    logger = kwargs['ti'].log
    logger.info("Starting the Neo4j data processing sem general")

    with Neo4jConnector(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) as neo4j_connector:
        load_and_execute_queries(queries_directory, neo4j_connector, logger)
        
        for label, property in indexes_to_create:
            neo4j_connector.create_index(label, property)
            logger.info(f"Index created on :{label}({property})")

        for part in range(1, part_count + 1):
            input_path = f"{base_input_path}{part}.json"
            logger.info(f"Checking for the existence of {input_path}")

            if os.path.exists(input_path):
                logger.info(f"Processing file: {input_path}")
                df = pd.read_json(input_path, orient='records', lines=True)
                total_rows = len(df)
                logger.info(f"Total rows in dataframe: {total_rows}")
                
                for start in range(0, total_rows, batch_size):
                    end = start + batch_size
                    logger.info(f"Processing batch from row {start} to {end}")
                    df_batch = df.iloc[start:end]
                    process_sem_general(df_batch, neo4j_connector, logger)
            else:
                logger.warning(f"File not found: {input_path}")

    logger.info("Completed processing all parts")

def insert_sem_additional_neo4j(base_input_path, batch_size=500, **kwargs):
    NEO4J_URI = "bolt://neo4j:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "project_pass123"
    part_count = 4

    logger = kwargs['ti'].log
    logger.info("Starting the Neo4j data processing sem additional")

    with Neo4jConnector(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) as neo4j_connector:
        for part in range(1, part_count + 1):
            input_path = f"{base_input_path}{part}.json"
            logger.info(f"Checking for the existence of {input_path}")

            if os.path.exists(input_path):
                logger.info(f"Processing file: {input_path}")
                df = pd.read_json(input_path, orient='records', lines=True)
                total_rows = len(df)
                logger.info(f"Total rows in dataframe: {total_rows}")
                
                for start in range(0, total_rows, batch_size):
                    end = start + batch_size
                    logger.info(f"Processing batch from row {start} to {end}")
                    df_batch = df.iloc[start:end]
                    process_sem_additional(df_batch, neo4j_connector, logger)
            else:
                logger.warning(f"File not found: {input_path}")

    logger.info("Completed processing all parts")
