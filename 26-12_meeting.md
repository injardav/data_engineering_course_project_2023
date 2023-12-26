# Meeting Summary

We met on Discord and covered the following topics:

## Data Enrichment
- Explored our current efforts to enrich data.
- Discussed challenges in data schema for better enrichment:
  - Issue with author uniqueness when using scholarly binaries for attribute retrieval (e.g., affiliation). May need to use only if unique results are returned.
  - Difficulty using dblp to retrieve data about publishers and authors due to discrepancies or absence in author details compared to the paper.

## Analytics Layer Discussion
- Discussed how to integrate DWH cubes and graphDB analytics.

## Agreed Next Tasks

### Task 1: Building Airflow Pipelines
1. **First Pipeline**: 
   - **Target**: Store the record in persistent storage (staging area) in a file with Avro specification.
   - **Actions**:
     - Retrieve a record from the dataset.
     - Drop papers with empty authors if DOI is empty.
     - Drop papers with titles shorter than 5 letters if DOI is empty.
     - Replace the ID field with a unique key.
     - Populate the General Category.
     - Drop documents with empty DOI.

2. **Second Pipeline**: 
   - **Target**: Process files from the staging area for DB/DWH and graphDB ingestion.
   - **Actions**:
     - Consume the CrossrefAPI based on paper DOI.
     - Format fields to be DB/graph friendly.

3. **Third Pipeline**: 
   - **Target**: Populate the Data Warehouse (DW).

4. **Fourth Pipeline**: 
   - **Target**: Populate the GraphDB.

### Task 2: Design DWH and graphDB Schemas
- Base the schema on the following dataset fields:
  - id, submitter, authors, title, comments, journal-ref, doi, report-no, categories, General_category, license, versions, update_date, authors_parsed, abstract, reference-count (int), publisher (string), issue (string), license_start (date_time), license_url (string), license_content-version (string), license_delay (int), short-container-title (string), container-title (string), type, is-referenced-by-count (int), author [given, family, sequence], reference [DOI, key, doi-asserted-by], language (string), Links[], deposited (datetime), score (int), ISSN [number, type], article-number (string).

### Task 3: Implement the ELK Stack
- Aim to enhance search capabilities within the solution.
