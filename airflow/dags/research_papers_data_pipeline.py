import os, json, zipfile, requests, time, pandas as pd
from scholarly import scholarly
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from kaggle.api.kaggle_api_extended import KaggleApi

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

logger = LoggingMixin().log

def get_unique_categories(row):
    return ' '.join(sorted(set(row.split())))

def load_category_mapping(file_path):
    logger.info("Loading category mapping json")
    with open(file_path, 'r') as file:
        return json.load(file)
    
def map_category(row, mapping):
    categories = row.split()
    return ' '.join(mapping.get(cat, cat) for cat in categories)

def get_paper_info_from_crossref(doi):
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
            return get_paper_info_from_crossref(doi)  # Recursive retry
        else:
            logger.error(f"HTTP error: {e}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}")
        return None

def consume_crossref(df):
    field_names = [
        'type', 'score', 'references_count',
        'references', 'publisher', 'issue',
        'license_start', 'license_url', 'license_content_version',
        'license_delay', 'short_container_title', 'container_title',
        'is_referenced_by_count', 'authors', 'language', 'links',
        'deposited', 'ISSN', 'ISSN_type', 'article_number', "URLs", "subject"
    ]
    for field in field_names:
        df[field] = None

    temp_df = df.head(10).copy()

    for index, row in temp_df.iterrows():
        paper_info = get_paper_info_from_crossref(row['doi'])
        if paper_info:
            for field in field_names:
                temp_df.at[index, field] = paper_info.get(field, None)
                df.at[index, field] = paper_info.get(field, None)  # Update the original dataframe as well

    # Save the temporary DataFrame (first 10 rows) to a CSV file or any other format
    temp_df.to_csv('temp_dataframe.csv', index=False)

def consume_scholarly(df):
    for index, row in df.iterrows():
        updated_authors = []
        for author in row['authors']:
            full_name = f"{author['given']} {author['family']}"
            
            search_query = scholarly.search_author(full_name)
            try:
                author_info = next(search_query)
            except StopIteration:
                updated_authors.append(author)
                continue
            
            updated_author = {
                'given': author.get('given', ''),
                'family': author.get('family', ''),
                'sequence': author.get('sequence', ''),
                'affiliation': author_info.get('affiliation', ''),
                'scholar_id': author_info.get('scholar_id', ''),
                'interests': author_info.get('interests', [])
            }
            updated_authors.append(updated_author)

        df.at[index, 'authors'] = updated_authors

def download_dataset():
    dataset_path = '/opt/airflow/dataset/arxiv.zip'

    if not os.path.exists(dataset_path):
        try:
            logger.info("Dataset did not exist, attempting to download")
            api = KaggleApi()
            api.authenticate()
            api.dataset_download_files('Cornell-University/arxiv', path='/opt/airflow/dataset/', unzip=False)
        except Exception as e:
            logger.error(f"Failed to download dataset: {e}")
            raise

def unzip_dataset():
    dataset_path = '/opt/airflow/dataset/arxiv.zip'
    extracted_path = '/opt/airflow/dataset/arxiv-metadata-oai-snapshot.json'

    if os.path.exists(dataset_path) and not os.path.exists(extracted_path):
        try:
            with zipfile.ZipFile(dataset_path, 'r') as zip_ref:
                zip_ref.extractall('/opt/airflow/dataset/')
        except zipfile.BadZipFile as e:
            logger.error(f"Failed to unzip dataset: {e}")
            raise

def transform_and_save_dataframe():
    file_path = '/opt/airflow/dataset/arxiv-metadata-oai-snapshot.json'
    output_path = '/opt/airflow/staging_area/arxiv_transformed.csv'

    if os.path.exists(file_path) and not os.path.exists(output_path):

        # df = pd.read_json(file_path, lines=True)
        # df = df.dropna(subset=['doi'])
        # df.reset_index(drop=True, inplace=True)
        # df.index += 1
        # df['id'] = df.index
        ### COMMENT THIS BACK IN IF YOU WANT TO READ IN ENTIRE DATASET

        rows_with_doi = []
        with open(file_path, 'r') as file:
            for line in file:
                row = json.loads(line)
                if 'doi' in row and row['doi']:
                    rows_with_doi.append(row)
                    if len(rows_with_doi) == 10:
                        break

        df = pd.DataFrame(rows_with_doi)
        df.reset_index(drop=True, inplace=True)
        df.index += 1
        df['id'] = df.index

        # General Category mapping
        df['categories'] = df['categories'].apply(get_unique_categories)
        category_mapping = load_category_mapping('/opt/airflow/data/category_mapping.json')
        df['general_category'] = df['categories'].apply(lambda x: map_category(x, category_mapping))
        df.drop('categories', axis=1, inplace=True)

        # Add Crossref data
        consume_crossref(df)

        # Add Scholarly data
        consume_scholarly(df)

        # Save the DataFrame to CSV
        df.to_csv(output_path, index=False)
        logger.info(f"DataFrame saved to {output_path}")
    else:
        logger.info(f"File {file_path} does not exist. Transformation and save operation skipped.")

with DAG(
    'download_transform_arxiv_data',
    default_args=default_args,
    description='DAG to download, transform and save arxiv dataset',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='download_dataset',
        python_callable=download_dataset,
    )

    t2 = PythonOperator(
        task_id='unzip_dataset',
        python_callable=unzip_dataset,
    )

    t3 = PythonOperator(
        task_id='transform_and_save_dataframe',
        python_callable=transform_and_save_dataframe,
    )

    t1 >> t2 >> t3
