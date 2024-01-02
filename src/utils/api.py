import requests, time, json, pandas as pd
from ratelimit import limits, sleep_and_retry
from utils.utils import load_dataset

def consume_crossref(df, logger):
    def get_paper_info_from_crossref(doi, logger):
        base_url = f'https://api.crossref.org/works/{doi}'
        user_agent = "Data_Engineering_course/1.0 (https://ut.ee; mailto:mahmouds@ut.ee)"
        headers = {"User-Agent": user_agent}
        logger.info(f"Calling CrossRef API with doi: {doi}")

        try:
            response = requests.get(base_url, headers=headers)
            response.raise_for_status()
            result = response.json()

            if result['status'] == 'ok':
                item = result["message"]
                logger.info(f"Response from CrossRef: {str(item)}")

                paper_info = {
                    "type": item.get('type', None),
                    "score": item.get('score', None),
                    "references_count": item.get('reference-count', None),
                    "references": item.get('reference', None),
                    "publisher": item.get('publisher', None),
                    "issue": item.get('issue', None),
                    "license": item.get('license', None),
                    "short_container_title": item.get('short-container-title', None),
                    "container_title": item.get('container-title', None),
                    "is_referenced_by_count": item.get('is-referenced-by-count', None),
                    "authors": item.get('author', None),
                    "language": item.get('language', None),
                    "links": item.get('link', None),
                    "deposited": item.get('deposited', {}).get('date-time', None),
                    "ISSN": item.get('ISSN', None),
                    "ISSN_type": item.get('issn-type', None),
                    "article_number": item.get('article-number', None),
                    "URLs": item.get('URL', None),
                    "subject": item.get('subject', None)
                }

                if 'license' in paper_info and isinstance(paper_info['license'], list) and paper_info['license']:
                    paper_info['license_start'] = paper_info['license'][0].get('start', {}).get('date-time', None)
                    paper_info['license_url'] = paper_info['license'][0].get('URL', None)
                    paper_info['license_content_version'] = paper_info['license'][0].get('content-version', None)
                    paper_info['license_delay'] = paper_info['license'][0].get('delay-in-days', None)

                return paper_info
            else:
                logger.info("No results found.")
                return None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Too Many Requests
                wait_time = int(e.response.headers.get("Retry-After", 60))
                logger.info(f"Rate limit reached, waiting for {wait_time} seconds")
                time.sleep(wait_time)
                return get_paper_info_from_crossref(doi, logger)  # Recursive retry
            else:
                logger.error(f"HTTP error: {e}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            return None
    
    field_names = [
        'type', 'score', 'references_count',
        'references', 'publisher', 'issue',
        'license_start', 'license_url', 'license_content_version',
        'license_delay', 'short_container_title', 'container_title',
        'is_referenced_by_count', 'authors', 'language', 'links',
        'deposited', 'ISSN', 'ISSN_type', 'article_number', 'URLs', 'subject'
    ]

    total_parts = 4
    for part in range(1, total_parts + 1):
        file_path = f"{'/opt/airflow/staging_area/arxiv_preprocessed_part_'}{part}.json"
        logger.info(f"Enriching file: {file_path}")
        df = load_dataset(file_path, logger)

        for field in field_names:
            df[field] = None

        for index, row in df.iterrows():
            paper_info = get_paper_info_from_crossref(row['doi'], logger)
            if paper_info:
                for field in field_names:
                    df.at[index, field] = paper_info.get(field, None)

class consume_semantic_scholar:
    def __init__(self, logger):
        self.logger = logger
        self.BASE_URL = 'https://api.semanticscholar.org/graph/v1'
        self.S2_API_KEY = "wSoW3gF4Uy65upxcchh9H9f5SQZyb2I75LRwCUPR"
        self.S2_PAPERS_BATCH_SIZE = 100
        self.paper_fields = [
            "paperId", "url", "title", "venue", "year", "abstract", "referenceCount",
            "citationCount", "influentialCitationCount", "isOpenAccess", "openAccessPdf",
            "fieldsOfStudy", "s2FieldsOfStudy", "publicationDate", "publicationVenue",
            "journal", "externalIds", "authors", "citations", "publicationTypes"
        ]

        self.author_fields = [
            'name', 'url', 'aliases', 'homepage', 
            'paperCount', 'citationCount', 'hIndex', 'affiliations'
        ]

        self.citation_fields = [
            'citationCount', 'influentialCitationCount', 'isOpenAccess', 
            'openAccessPdf', 'fieldsOfStudy', 's2FieldsOfStudy', 
            'publicationTypes', 'publicationDate', 'journal'
        ]

        self.reference_fields = [
            'citationCount', 'influentialCitationCount', 'isOpenAccess', 
            'openAccessPdf', 'fieldsOfStudy', 's2FieldsOfStudy', 
            'publicationTypes', 'publicationDate', 'journal'
        ]

    def make_api_request(self, url, data=None, method='GET'):
        """
        Generic function to make API requests to Semantic Scholar.
        """
        headers = {'x-api-key': self.S2_API_KEY}
        if method.upper() == 'GET':
            response = requests.get(url, headers=headers, params=data)
        elif method.upper() == 'POST':
            response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            try:
                return response.json()
            except json.JSONDecodeError:
                self.logger.error("Failed to decode JSON from response")
                return []
        else:
            self.logger.error(f"Error (response code {response.status_code}) in API request: {response.text} with url: {url}")
            return []

    @sleep_and_retry
    @limits(calls=1, period=1) # 1 request per second
    def fetch_papers(self, ids, fields):
        """
        Fetches batch of papers from Semantic Scholar API for given IDs.
        """
        url = f"{self.BASE_URL}/paper/batch"
        params = {
            'fields': ','.join(fields),
            'ids': ids
        }
        return self.make_api_request(url, params, 'POST')

    @sleep_and_retry
    @limits(calls=10, period=1) # 10 request per second
    def fetch_authors(self, paper_id, fields):
        """
        Fetches authors from Semantic Scholar API for a given paper ID.
        """
        url = f"{self.BASE_URL}/paper/{paper_id}/authors"
        params = {'fields': ','.join(fields)}
        return self.make_api_request(url, params)

    @sleep_and_retry
    @limits(calls=10, period=1) # 10 request per second
    def fetch_citations(self, paper_id, fields):
        """
        Fetches citations from Semantic Scholar API for a given paper ID.
        """
        url = f"{self.BASE_URL}/paper/{paper_id}/citations"
        params = {'fields': ','.join(fields)}
        return self.make_api_request(url, params)

    @sleep_and_retry
    @limits(calls=10, period=1) # 10 request per second
    def fetch_references(self, paper_id, fields):
        """
        Fetches references from Semantic Scholar API for a given paper ID.
        """
        url = f"{self.BASE_URL}/paper/{paper_id}/references"
        params = {'fields': ','.join(fields)}
        return self.make_api_request(url, params)
        
    def format_id(self, row, id_type):
        if id_type == 'arxiv' and pd.notna(row['arxiv']):
            return f"ARXIV:{row['arxiv']}"
        elif id_type == 'doi' and pd.notna(row['doi']):
            return f"DOI:{row['doi']}"
        return None 
    
def semantic_general(base_file_path, base_file_destination, **kwargs):
    """
    Fetches data from batch papers endpoint and processes general paper fields.
    """
    total_parts = 4
    no_response_indices = []
    logger = kwargs['ti'].log
    logger.info("Starting general data enrichment process with Semantic Scholar API")
    s2 = consume_semantic_scholar(logger)

    def process_paper(paper, row_index):
        if row_index >= len(df):
            return

        for field in s2.paper_fields:
            if field not in df.columns:
                df[field] = None

            df.at[row_index, field] = paper.get(field)

    def process_batch(batch, batch_index):
        logger.info(f"Processing batch {batch_index + 1}/{len(batches)}")
        arxiv_ids = [s2.format_id(row, 'arxiv') for row in batch if s2.format_id(row, 'arxiv')]
        response = s2.fetch_papers(arxiv_ids, s2.paper_fields) 

        retry_ids = [arxiv_ids[i] for i, paper in enumerate(response) if paper is None]
        doi_ids = [s2.format_id(batch[i], 'doi') for i in range(len(batch)) if arxiv_ids[i] in retry_ids]
        response_retry = s2.fetch_papers(doi_ids, s2.paper_fields)

        for i, paper in enumerate(response + response_retry):
            actual_index = batch_index * s2.S2_PAPERS_BATCH_SIZE + i
            if paper is None and (i < len(response) or response_retry[i - len(response)] is None):
                no_response_indices.append(actual_index)

        logger.info(f"Completed processing batch {batch_index + 1}/{len(batches)}")

    for part in range(1, total_parts + 1):
        file_path = f"{base_file_path}{part}.json"
        output_path = f"{base_file_destination}{part}.json"
        logger.info(f"Processing file: {file_path}")

        df = load_dataset(file_path, logger)
        all_rows = df.to_dict('records')
        batches = [all_rows[i:i + s2.S2_PAPERS_BATCH_SIZE] for i in range(0, len(all_rows), s2.S2_PAPERS_BATCH_SIZE)]

        for batch_index, batch in enumerate(batches):
            process_batch(batch, batch_index)

        # Drop rows with no response
        df.drop(index=no_response_indices, inplace=True)

        df.to_json(output_path, orient='records', lines=True, force_ascii=False)
        logger.info(f"Processed data saved to: {output_path}")

def semantic_additional(base_file_path, base_file_destination, **kwargs):
    """
    Fetches authors, citations, and references data for each paper.
    """
    total_parts = 4
    logger = kwargs['ti'].log
    logger.info("Starting additional data enrichment process with Semantic Scholar API")
    s2 = consume_semantic_scholar(logger)

    def process_paper(paper_id, row_index):
        if row_index >= len(df):
            return
        
        authors = s2.fetch_authors(paper_id, s2.author_fields)
        citations = s2.fetch_citations(paper_id, s2.citation_fields)
        references = s2.fetch_references(paper_id, s2.reference_fields)

        df.at[row_index, 'authors_data'] = authors.get('data')
        df.at[row_index, 'citations_data'] = citations.get('data')
        df.at[row_index, 'references_data'] = references.get('data')

    def process_batch(batch, batch_index):
        logger.info(f"Processing batch {batch_index + 1}/{len(batches)}")
        
        for row_index, row in enumerate(batch):
            paper_id = row.get('id')
            if paper_id:
                process_paper(paper_id, batch_index * s2.S2_PAPERS_BATCH_SIZE + row_index)

        logger.info(f"Completed processing batch {batch_index + 1}/{len(batches)}")

    for part in range(1, total_parts + 1):
        file_path = f"{base_file_path}{part}.json"
        output_path = f"{base_file_destination}{part}.json"
        logger.info(f"Processing file: {file_path}")

        df = load_dataset(file_path, logger)
        all_rows = df.to_dict('records')
        batches = [all_rows[i:i + s2.S2_PAPERS_BATCH_SIZE] for i in range(0, len(all_rows), s2.S2_PAPERS_BATCH_SIZE)]

        for batch_index, batch in enumerate(batches):
            process_batch(batch, batch_index)

        # Save the processed data for the current part
        df.to_json(output_path, orient='records', lines=True, force_ascii=False)
        logger.info(f"Enriched data for part {part} saved to: {output_path}")