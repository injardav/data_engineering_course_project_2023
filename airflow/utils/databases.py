import uuid
from datetime import datetime
from neo4j import GraphDatabase

class Neo4jConnector:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def execute_query(self, query, parameters=None):
        with self._driver.session() as session:
            result = session.run(query, parameters)
            return result

def process_batch(df_batch, neo4j_connector):
    # Prepare data for UNWIND
    batch_data = df_batch.to_dict('records')
    author_uuids = {}
    authors_data = []
    references_data = []
    versions_data = []
    for row in batch_data:
        for author in row.get('authors', []):
            # Create a unique key for each author (e.g., using first and last name)
            author_key = (author.get('first_name'), author.get('family_name'))

            # Check if the author already has a UUID, otherwise generate a new one
            if author_key not in author_uuids:
                author_uuids[author_key] = str(uuid.uuid4())

            authors_data.append({
                'publication_id': row['id'],
                'author_id': author_uuids[author_key],
                'first_name': author.get('first_name'),
                'family_name': author.get('family_name')
            })

        for reference in row.get('references', []):
            references_data.append({
                'publication_id': row['id'],
                'ref_doi': reference.get('DOI'),
                'ref_key': reference.get('key'),
                'ref_doi_asserted_by': reference.get('doi-asserted-by')
            })

        for version in row.get('versions', []):
            versions_data.append({
                'publication_id': row['id'],
                'version_id': str(uuid.uuid4()),
                'created_time': version.get('created'),
                'version': version.get('version')
            })

    # Cypher query with UNWIND
    query = """
        UNWIND $versions_data AS ver        
        CREATE (:Dim_Pub_Version {
            publication_id: ver.publication_id,
            version_id: ver.version_id,
            created_time: ver.created_time,
            version: ver.version
        })

        UNWIND $references_data AS ref
        CREATE(:Dim_References {
            publication_id: ref.publication_id,
            ref_doi: ref.ref_doi,
            ref_key: ref.ref_key,
            ref_doi_asserted_by: ref.ref_doi_asserted_by
        })
        
        UNWIND $authors_data AS author
        CREATE (:Dim_Authors {
            author_id: author.author_id,
            publication_id: author.publication_id,
            first_name: author.first_name,
            family_name: author.family_name
        })

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

        CREATE (:Dim_Author_Affiliation {
            publication_id: row.id,
            affiliation: row.affiliation,
            is_current: row.is_current,
            start_date: null,
            end_date: null
        })

        CREATE (:Dim_Publisher {
            publication_id: row.id,
            publisher_name: row.publisher
        })

        CREATE (:Dim_Publisher_SN {
            publication_id: row.id,
            issn_number: row.ISSN,
            issn_type: row.ISSN_type
        })

        CREATE (:Dim_License {
            publication_id: row.id,
            license_start: row.license_start,
            license_url: row.license_url,
            license_content_version: row.license_content_version,
            license_delay: row.license_delay
        })

        CREATE (:Publication_Fact {
            publication_id: row.id,
            references_count: row.references_count,
            score: row.score,
            doi: row.doi,
            start_date: null
        })

        MATCH (pub:Dim_Publication {publication_id: row.id})
        MATCH (auth:Dim_Authors {publication_id: row.id})
        MERGE (pub)-[:AUTHORED_BY]->(auth)

        MATCH (auth:Dim_Authors {publication_id: row.id})
        MATCH (aff:Dim_Author_Affiliation {publication_id: row.id})
        MERGE (auth)-[:AFFILIATED_WITH]->(aff)

        MATCH (pub:Dim_Publication {publication_id: row.id})
        MATCH (serial:Dim_Publisher_SN {publication_id: row.id})
        MERGE (pub)-[:HAS_ISSN]->(serial)

        MATCH (pub:Dim_Publication {publication_id: row.id})
        MATCH (lic:Dim_License {publication_id: row.id})
        MERGE (pub)-[:HAS_LICENSE]->(lic)

        MATCH (pub:Dim_Publication {publication_id: row.id})
        MATCH (ref:Dim_References {publication_id: row.id})
        MERGE (pub)-[:HAS_REFERENCE]->(ref)

        MATCH (pub:Dim_Publication {publication_id: row.id})
        MATCH (ver:Dim_Pub_Version {publication_id: row.id})
        MERGE (pub)-[:HAS_VERSION]->(ver)

        MATCH (fact:Publication_Fact {doi: row.doi})
        MATCH (pub:Dim_Publication {doi: row.doi})
        MERGE (fact)-[:BASED_ON_PUBLICATION]->(pub)
    """

    # Execute the query
    neo4j_connector.execute_query(query, {'batch': batch_data, 'authors_data': authors_data, 'references_data': references_data, 'versions_data': versions_data})

def insert_into_neo4j(df, batch_size=100):
    NEO4J_URI = "bolt://neo4j:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "project_pass123"

    with Neo4jConnector(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) as neo4j_connector:
        for start in range(0, len(df), batch_size):
            df_batch = df.iloc[start:start+batch_size]
            process_batch(df_batch, neo4j_connector)
