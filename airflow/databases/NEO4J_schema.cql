CREATE (:Dim_Publication {
    publication_id: 1,
    submitter: 'John Doe',
    article_number: '12345',
    title: 'Sample Title',
    journal_ref: 'Journal ABC',
    general_category: 'Science',
    type: 'Research',
    issue: 'Issue 1',
    language: 'English',
    short_container_title: 'Short Title',
    container_title: 'Full Title',
    is_referenced_by_count: 10,
    is_current: true,
    start_date: datetime('2023-01-01T00:00:00Z'),
    end_date: datetime('2023-12-31T23:59:59Z')
});

CREATE (:Dim_Authors {
    publication_id: 1
    author_id: 1,
    affiliation_id: 1,
    first_name: 'Jane',
    family_name: 'Doe'
});

CREATE (:Dim_Author_Affiliation {
    affiliation_id: 1,
    affiliation: 'University XYZ',
    is_current: true,
    start_date: datetime('2023-01-01T00:00:00Z'),
    end_date: datetime('2023-12-31T23:59:59Z')
});

CREATE (:Dim_Publisher {
    publisher_id: 1,
    publisher_name: 'ABC Publishers'
});

CREATE (:Dim_Publisher_SN {
    publisher_id: 1,
    issn_number: '1234-5678',
    issn_type: 'Print'
});

CREATE (:Dim_License {
    license_id: 1,
    license_start: datetime('2023-01-01T00:00:00Z'),
    license_url: 'http://sample-license.com',
    license_content_version: '1.0',
    license_delay: 7
});

CREATE (:Dim_References {
    publication_id: 1,
    doi: 'doi:12345/6789',
    key: 'reference_key',
    doi_asserted_by: 'John Doe'
});

CREATE (:Dim_Pub_Version {
    publication_id: 1,
    version_id: 'version_1',
    created_time: datetime('2023-01-01T12:00:00Z')
});

// Create the fact node
CREATE (:Publication_Fact {
    id: 1,
    reference_count: 20,
    score: 85,
    doi: 'doi:12345/6789',
    start_date: datetime('2023-01-01T00:00:00Z')
});

// Create relationships between nodes
MATCH (pub:Dim_Publication), (auth:Dim_Authors)
WHERE pub.publication_id = 1 AND auth.author_id = 1
CREATE (pub)-[:AUTHORED_BY]->(auth);

MATCH (auth:Dim_Authors), (aff:Dim_Author_Affiliation)
WHERE auth.publication_id = 1 AND aff.affiliation_id = 1
CREATE (auth)-[:AFFILIATED_WITH]->(aff);

MATCH (pub:Dim_Publication), (serial:Dim_Publisher_SN)
WHERE pub.publication_id = 1 AND serial.publication_id = 1
CREATE (pub)-[:HAS_ISSN]->(serial);

MATCH (pub:Dim_Publication), (lic:Dim_License)
WHERE pub.publication_id = 1 AND lic.license_id = 1
CREATE (pub)-[:HAS_LICENSE]->(lic);

MATCH (pub:Dim_Publication), (ref:Dim_References)
WHERE pub.publication_id = 1 AND ref.publication_id = 1
CREATE (pub)-[:HAS_REFERENCE]->(ref);

MATCH (pub:Dim_Publication), (ver:Dim_Pub_Version)
WHERE pub.publication_id = 1 AND ver.publication_id = 1
CREATE (pub)-[:HAS_VERSION]->(ver);

MATCH (fact:Publication_Fact), (pub:Dim_Publication)
WHERE fact.doi = 'doi:12345/6789' AND pub.doi = 'doi:12345/6789'
CREATE (fact)-[:BASED_ON_PUBLICATION]->(pub);
