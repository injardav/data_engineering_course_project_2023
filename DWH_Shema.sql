-- Create a schema
CREATE SCHEMA data_warehouse;

-- Set the search path to the new schema
SET search_path TO data_warehouse;

-- Create a Star Schema within the specified DWH
CREATE TABLE Dim_Publication (
    pid SERIAL PRIMARY KEY,
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

CREATE TABLE Dim_Authors (
    aid SERIAL PRIMARY KEY,
    aff_id INTEGER REFERENCES Dim_Author_Affiliation(aff_id),
    first_name VARCHAR(255),
    family_name VARCHAR(255)
);

CREATE TABLE Dim_Author_Affiliation (
    aff_id SERIAL PRIMARY KEY,
    affiliation VARCHAR(255),
    is_current BOOLEAN,
    start_date TIMESTAMP,
    end_date TIMESTAMP
);

CREATE TABLE Dim_Publisher (
    pub_id SERIAL PRIMARY KEY,
    publisher_name VARCHAR(255)
);

CREATE TABLE Dim_Publish_SN (
    pid INTEGER REFERENCES Dim_Publication(pid),
    issn_number VARCHAR(255),
    issn_type VARCHAR(255),
    PRIMARY KEY (pid, issn_number)
);

CREATE TABLE Dim_License (
    lid SERIAL PRIMARY KEY,
    license_start TIMESTAMP,
    license_url VARCHAR(255),
    license_content_version VARCHAR(255),
    license_delay INTEGER
);

CREATE TABLE Dim_References (
    pid INTEGER REFERENCES Dim_Publication(pid),
    doi VARCHAR(255),
    key VARCHAR(255),
    doi_asserted_by VARCHAR(255),
    PRIMARY KEY (pid, doi)
);

CREATE TABLE Dim_Pub_Version (
    pid INTEGER REFERENCES Dim_Publication(pid),
    vid VARCHAR(255),
    created_time TIMESTAMP,
    PRIMARY KEY (pid, vid)
);

-- Create Fact Table
CREATE TABLE Publication_Fact (
    id SERIAL PRIMARY KEY,
    reference_count INTEGER,
    score INTEGER,
    doi VARCHAR(255) REFERENCES Dim_Publication(doi),
    start_date TIMESTAMP
);
-- Reset the search path to the default
RESET search_path;
