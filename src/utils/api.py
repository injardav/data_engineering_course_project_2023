import requests, time, json, pandas as pd
from ratelimit import limits, sleep_and_retry
from utils.utils import load_dataset

S2_API_KEY = "wSoW3gF4Uy65upxcchh9H9f5SQZyb2I75LRwCUPR"
S2_PAPERS_BATCH_SIZE = 500

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

def consume_semantic_scholar(**kwargs):
    def format_id(row, id_type):
        """
        Formats the ID based on whether it's an ArXiv ID or a DOI.
        """
        if id_type == 'arxiv' and pd.notna(row['arxiv']):
            return f"ARXIV:{row['arxiv']}"
        elif id_type == 'doi' and pd.notna(row['doi']):
            return f"DOI:{row['doi']}"
        else:
            logger.info("ARXIV and DOI were not found for paper. This should not happen")
            return None

    def chunker(seq, size):
        """
        Divides the data into chunks of specified size.
        """
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))
    
    def fetch_paper_with_fallback(row):
        """
        Fetches a single paper, first trying with ARXIV ID, then with DOI if the first attempt fails.
        """
        formatted_id = format_id(row, 'arxiv')
        paper = fetch_papers([formatted_id], fields)
        if paper is None or not paper:
            logger.info("Paper not found with ARXIV, attempting with DOI")
            formatted_id = format_id(row, 'doi')
            paper = fetch_papers([formatted_id], fields)
        return paper
    
    logger = kwargs['ti'].log
    logger.info("Starting data enrichment process with Semantic Scholar API")

    @sleep_and_retry
    @limits(calls=1, period=1)  # 1 request per second
    def fetch_papers(ids, fields):
        """
        Fetches papers from Semantic Scholar API for given IDs.
        """
        response = requests.post(
            'https://api.semanticscholar.org/graph/v1/paper/batch',
            headers={'x-api-key': S2_API_KEY},
            params={'fields': ','.join(fields)},
            json={'ids': ids}
        )
        if response.status_code == 200:
            try:
                return response.json()
            except json.JSONDecodeError:
                logger.error("Failed to decode JSON from response")
                return []
        else:
            logger.error(f"Error fetching papers: {response.text}")
            return []

    base_file_path = '/opt/airflow/staging_area/arxiv_preprocessed_part_'
    base_output_path = '/opt/airflow/staging_area/arxiv_enriched_part_'
    total_parts = 4
    fields = [
        "publicationTypes", "referenceCount", "references", "journal", "externalIds",
        "venue", "citationCount", "authors", "url", "openAccessPdf", "publicationDate"
    ]

    for part in range(1, total_parts + 1):
        file_path = f"{base_file_path}{part}.json"
        logger.info(f"Enriching file: {file_path}")
        
        df = load_dataset(file_path, logger)

        # Initialize fields in the DataFrame
        for field in fields:
            df[field] = None

        all_rows = df.to_dict('records')
        total_chunks = (len(all_rows) - 1) // S2_PAPERS_BATCH_SIZE + 1
        current_chunk = 1
        for chunk in chunker(all_rows, S2_PAPERS_BATCH_SIZE):
            logger.info(f"Enriching chunk {current_chunk} of {total_chunks} for file part {part}")
            papers = [fetch_paper_with_fallback(row) for row in chunk]

            for paper in papers:
                if isinstance(paper, dict):  # Ensure 'paper' is a dictionary
                    paper_doi = paper.get('externalIds', {}).get('DOI')
                    if paper_doi:
                        matching_indices = df.index[df['doi'] == paper_doi].tolist()
                        for index in matching_indices:
                            for field in fields:
                                df.at[index, field] = paper.get(field, None)
                else:
                    logger.error(f"Invalid paper data format: {paper}")
            current_chunk += 1
        logger.info(f"Completed enriching for file part {part}")

        # Save the processed subset
        output_path = f"{base_output_path}{part}.json"
        df.to_json(output_path, orient='records', lines=True)
        logger.info(f"Subset {part} of enriched file saved to {output_path}")
