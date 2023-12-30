import requests, time, json, pandas as pd

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
    
def consume_crossref(df, logger):
    field_names = [
        'type', 'score', 'references_count',
        'references', 'publisher', 'issue',
        'license_start', 'license_url', 'license_content_version',
        'license_delay', 'short_container_title', 'container_title',
        'is_referenced_by_count', 'authors', 'language', 'links',
        'deposited', 'ISSN', 'ISSN_type', 'article_number', 'URLs', 'subject'
    ]
    for field in field_names:
        df[field] = None

    for index, row in df.iterrows():
        paper_info = get_paper_info_from_crossref(row['doi'], logger)
        if paper_info:
            for field in field_names:
                df.at[index, field] = paper_info.get(field, None)

def consume_semantic_scholar(df, logger):
    def format_id(row):
        """
        Formats the ID based on whether it's an ArXiv ID or a DOI.
        """
        if pd.notna(row['id']):
            return f"ARXIV:{row['id']}"
        elif pd.notna(row['doi']):
            return f"DOI:{row['doi']}"
        else:
            return None

    def chunker(seq, size):
        """
        Divides the data into chunks of specified size.
        """
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def fetch_papers(ids):
        """
        Fetches papers from Semantic Scholar API for given IDs.
        """
        response = requests.post(
            'https://api.semanticscholar.org/graph/v1/paper/batch',
            params={'fields': 'referenceCount,citationCount,title'},
            json={'ids': ids}
        )
        return json.dumps(response.json())

    # Format the IDs and drop rows without valid IDs
    df['formatted_id'] = df.apply(format_id, axis=1)
    df.dropna(subset=['formatted_id'], inplace=True)

    results = []

    for chunk in chunker(df['formatted_id'].tolist(), 100):
        logger.info(f"Fetching papers from semantic scholar chunk {chunk}")
        papers = fetch_papers(chunk)
        logger.info(f"Response received from semantic scholar {str(papers)}")
        results.extend(papers)

        logger.info("Sleeping for 5 minutes due to request rate limit reached")
        time.sleep(5 * 60 / 100)  # 5 minutes for 100 requests

    # Convert results to DataFrame and merge with original df
    results_df = pd.DataFrame(results)
    df = pd.merge(df, results_df, left_on='formatted_id', right_on='paperId', how='left')

    df.drop(columns=['formatted_id'], inplace=True)
