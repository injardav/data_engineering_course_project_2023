// Create relationships between nodes
// Create nodes for dimensions
// Create nodes for dimensions
CREATE (:Dim_Publication {
    pid: 1,
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
    aid: 1,
    aff_id: 1,
    first_name: 'Jane',
    family_name: 'Doe'
});

CREATE (:Dim_Author_Affiliation {
    aff_id: 1,
    affiliation: 'University XYZ',
    is_current: true,
    start_date: datetime('2023-01-01T00:00:00Z'),
    end_date: datetime('2023-12-31T23:59:59Z')
});

CREATE (:Dim_Publisher {
    pub_id: 1,
    publisher_name: 'ABC Publishers'
});

CREATE (:Dim_Publish_SN {
    pid: 1,
    issn_number: '1234-5678',
    issn_type: 'Print'
});

CREATE (:Dim_License {
    lid: 1,
    license_start: datetime('2023-01-01T00:00:00Z'),
    license_url: 'http://sample-license.com',
    license_content_version: '1.0',
    license_delay: 7
});

CREATE (:Dim_References {
    pid: 1,
    doi: 'doi:12345/6789',
    key: 'reference_key',
    doi_asserted_by: 'John Doe'
});

CREATE (:Dim_Pub_Version {
    pid: 1,
    vid: 'version_1',
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
MATCH (p:Dim_Publication), (a:Dim_Authors)
WHERE p.pid = 1 AND a.aid = 1
CREATE (p)-[:AUTHORED_BY]->(a);

MATCH (a:Dim_Authors), (aff:Dim_Author_Affiliation)
WHERE a.aff_id = 1 AND aff.aff_id = 1
CREATE (a)-[:AFFILIATED_WITH]->(aff);

MATCH (p:Dim_Publication), (s:Dim_Publish_SN)
WHERE p.pid = 1 AND s.pid = 1
CREATE (p)-[:HAS_ISSN]->(s);

MATCH (p:Dim_Publication), (l:Dim_License)
WHERE p.pid = 1 AND l.lid = 1
CREATE (p)-[:HAS_LICENSE]->(l);

MATCH (p:Dim_Publication), (r:Dim_References)
WHERE p.pid = 1 AND r.pid = 1
CREATE (p)-[:HAS_REFERENCE]->(r);

MATCH (p:Dim_Publication), (v:Dim_Pub_Version)
WHERE p.pid = 1 AND v.pid = 1
CREATE (p)-[:HAS_VERSION]->(v);

MATCH (f:Publication_Fact), (p:Dim_Publication)
WHERE f.doi = 'doi:12345/6789' AND p.doi = 'doi:12345/6789'
CREATE (f)-[:BASED_ON_PUBLICATION]->(p);
