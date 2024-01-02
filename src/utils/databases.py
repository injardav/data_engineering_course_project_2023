import uuid, os, pandas as pd
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
        
    def create_index(self, label, property):
        create_index_query = f"CREATE INDEX ON :{label}({property})"
        with self._driver.session() as session:
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

def process_batch(df_batch, neo4j_connector, logger):
    logger.info(f"Starting to process batch of length {len(df_batch)}")
    batch_data = df_batch.to_dict('records')
    author_uuids = {}
    authors_data = []
    references_data = []
    versions_data = []
    for row in batch_data:
        logger.debug(f"Processing row: {row['id']}")
        try:
            row['ISSN_type_values'] = [item['value'] for item in row['ISSN_type']]
            row['ISSN_type_types'] = [item['type'] for item in row['ISSN_type']]

            authors = row.get('authors') or []
            for author in authors:
                # Create a unique key for each author (e.g., using first and last name)
                author_key = (author.get('given'), author.get('family'))

                # Check if the author already has a UUID, otherwise generate a new one
                if author_key not in author_uuids:
                    author_uuids[author_key] = str(uuid.uuid4())

                authors_data.append({
                    'publication_id': row['id'],
                    'author_id': author_uuids[author_key],
                    'first_name': author.get('given'),
                    'family_name': author.get('family')
                })

            references = row.get('references') or []
            for reference in references:
                references_data.append({
                    'publication_id': row['id'],
                    'ref_doi': reference.get('DOI'),
                    'ref_key': reference.get('key'),
                    'ref_doi_asserted_by': reference.get('doi-asserted-by')
                })

            versions = row.get('versions') or []
            for version in versions:
                versions_data.append({
                    'publication_id': row['id'],
                    'version_id': str(uuid.uuid4()),
                    'created_time': version.get('created'),
                    'version': version.get('version')
                })

        except Exception as e:
            logger.error(f"Error processing row {row['id']}: {e}")

    try:
        versions_query = """
            UNWIND $versions_data AS ver        
            CREATE (:Dim_Pub_Version {
                publication_id: ver.publication_id,
                version_id: ver.version_id,
                created_time: ver.created_time,
                version: ver.version
            })
        """
        logger.info("Executing versions query")
        neo4j_connector.execute_query(versions_query, {'versions_data': versions_data})

        authors_query = """
            UNWIND $authors_data AS author
            CREATE (:Dim_Authors {
                author_id: author.author_id,
                publication_id: author.publication_id,
                first_name: author.first_name,
                family_name: author.family_name
            })
        """
        logger.info("Executing authors query")
        neo4j_connector.execute_query(authors_query, {'authors_data': authors_data})

        references_query = """
            UNWIND $references_data AS ref
            CREATE(:Dim_References {
                publication_id: ref.publication_id,
                ref_doi: ref.ref_doi,
                ref_key: ref.ref_key,
                ref_doi_asserted_by: ref.ref_doi_asserted_by
            })
        """
        logger.info("Executing references query")
        neo4j_connector.execute_query(references_query, {'references_data': references_data})

        publications_query = """
            UNWIND $batch AS row
            CREATE (:Dim_Publication {
                publication_id: row.id,
                submitter: row.submitter,
                article_number: row.article_number,
                title: row.title,
                journal_ref: row.journal,
                general_category: row.general_category,
                type: row.type,
                issue: row.issue,
                language: row.language,
                short_container_title: row.short_container_title,
                container_title: row.container_title,
                is_referenced_by_count: row.is_referenced_by_count,
                is_current: row.is_current,
                start_date: null,
                end_date: null
            })
        """
        logger.info("Executing publications query")
        neo4j_connector.execute_query(publications_query, {'batch': batch_data})

        auth_affiliations_query = """
            UNWIND $batch AS row
            CREATE (:Dim_Author_Affiliation {
                publication_id: row.id,
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
            CREATE (:Dim_Publisher {
                publication_id: row.id,
                publisher_name: row.publisher
            })
        """
        logger.info("Executing publishers query")
        neo4j_connector.execute_query(publishers_query, {'batch': batch_data})

        publisher_sn_query = """
            UNWIND $batch AS row
            CREATE (:Dim_Publisher_SN {
                publication_id: row.id,
                issn_values: row.ISSN_type_values,
                issn_types: row.ISSN_type_types
            })
        """
        logger.info("Executing publishers issn query")
        neo4j_connector.execute_query(publisher_sn_query, {'batch': batch_data})

        licenses_query = """
            UNWIND $batch AS row
            CREATE (:Dim_License {
                publication_id: row.id,
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
            CREATE (:Publication_Fact {
                publication_id: row.id,
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
            MATCH (pub:Dim_Publication {publication_id: row.id})
            MATCH (auth:Dim_Authors {publication_id: row.id})
            CREATE (pub)-[:AUTHORED_BY]->(auth)
        """
        logger.info("Executing relationships query 1")
        neo4j_connector.execute_query(relationships_query_1, {'batch': batch_data})

        relationships_query_2 = """
            UNWIND $batch AS row
            MATCH (auth:Dim_Authors {publication_id: row.id})
            MATCH (aff:Dim_Author_Affiliation {publication_id: row.id})
            CREATE (auth)-[:AFFILIATED_WITH]->(aff)
        """
        logger.info("Executing relationships query 2")
        neo4j_connector.execute_query(relationships_query_2, {'batch': batch_data})

        relationships_query_3 = """
            UNWIND $batch AS row
            MATCH (serial:Dim_Publisher_SN {publication_id: row.id})
            CREATE (pub)-[:HAS_ISSN]->(serial)
        """
        logger.info("Executing relationships query 3")
        neo4j_connector.execute_query(relationships_query_3, {'batch': batch_data})

        relationships_query_4 = """
            UNWIND $batch AS row
            MATCH (lic:Dim_License {publication_id: row.id})
            CREATE (pub)-[:HAS_LICENSE]->(lic)
        """
        logger.info("Executing relationships query 4")
        neo4j_connector.execute_query(relationships_query_4, {'batch': batch_data})

        relationships_query_5 = """
            UNWIND $batch AS row
            MATCH (ref:Dim_References {publication_id: row.id})
            CREATE (pub)-[:HAS_REFERENCE]->(ref)
        """
        logger.info("Executing relationships query 5")
        neo4j_connector.execute_query(relationships_query_5, {'batch': batch_data})
        
        relationships_query_6 = """
            UNWIND $batch AS row
            MATCH (ver:Dim_Pub_Version {publication_id: row.id})
            CREATE (pub)-[:HAS_VERSION]->(ver)
        """
        logger.info("Executing relationships query 6")
        neo4j_connector.execute_query(relationships_query_6, {'batch': batch_data})

        relationships_query_7 = """
            UNWIND $batch AS row
            MATCH (fact:Publication_Fact {doi: row.doi})
            MATCH (pub:Dim_Publication {doi: row.doi})
            CREATE (fact)-[:BASED_ON_PUBLICATION]->(pub)
        """
        logger.info("Executing relationships query 7")
        neo4j_connector.execute_query(relationships_query_7, {'batch': batch_data})

        logger.info("Successfully processed and inserted batch data into Neo4j")

    except Exception as e:
        logger.error(f"Error executing Neo4j queries: {e}")

def insert_into_neo4j(queries_directory, base_input_path, batch_size=500, **kwargs):
    NEO4J_URI = "bolt://neo4j:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "project_pass123"
    part_count = 4

    indexes_to_create = [
        ("Dim_Publication", "publication_id"),
        ("Dim_Authors", "publication_id"),
        ("Dim_Author_Affiliation", "publication_id"),
        ("Dim_Publisher_SN", "publication_id"),
        ("Dim_License", "publication_id"),
        ("Dim_References", "publication_id"),
        ("Dim_Pub_Version", "publication_id"),
        ("Publication_Fact", "doi")
    ]

    logger = kwargs['ti'].log
    logger.info("Starting the Neo4j data processing")

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
                    process_batch(df_batch, neo4j_connector, logger)
            else:
                logger.warning(f"File not found: {input_path}")

    logger.info("Completed processing all parts")

def insert_into_postgres(**kwargs):
    ...
