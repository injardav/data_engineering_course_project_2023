import os, zipfile, uuid, json, pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

def load_dataset(file_path, subset=False, start_row=0, rows=10):
    """
    Loads a specific subset of the dataset starting from `start_row` and 
    loading `rows` number of rows, or the entire dataset if `subset` is False.
    Applies dropna to remove rows where both 'doi' and 'id' are NaN.
    """
    data = []
    current_row = 0

    with open(file_path, 'r') as file:
        for line in file:
            if subset and current_row >= start_row + rows:
                break

            row = json.loads(line)
            if subset:
                if ('doi' in row and row['doi']) and ('id' in row and row['id']):
                    if current_row >= start_row:
                        data.append(row)
                    current_row += 1
            else:
                data.append(row)

    df = pd.DataFrame(data)
    if not subset:
        df = df.dropna(subset=['id', 'doi'], how='all')
    return df

def get_unique_categories(row):
    return ' '.join(sorted(set(row.split())))

def load_category_mapping(file_path, logger):
    logger.info("Loading category mapping json")
    with open(file_path, 'r') as file:
        return json.load(file)
    
def map_category(row, mapping, logger):
    categories = row.split()
    logger.info(f"Mapping for unique categories: {categories}")
    return ' '.join(mapping.get(cat, cat) for cat in categories)

def map_general_categories(df, logger):
    logger.info("Mapping to general categories")
    df['categories'] = df['categories'].apply(get_unique_categories)
    category_mapping = load_category_mapping('/opt/airflow/resources/category_mapping.json', logger)
    df['general_category'] = df['categories'].apply(lambda x: map_category(x, category_mapping, logger))
    df.drop('categories', axis=1, inplace=True)

def handle_id(df):
    """
    Rename `id` to `arxiv` and create correct `id` field with UUID values
    """
    df.rename(columns={'id': 'arxiv'}, inplace=True)
    df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]

def handle_authors(df):
    """
    Change all empty authors values to empty list for later parsing
    """
    df['authors'] = df['authors'].apply(lambda x: [] if pd.isna(x) or x.strip() == '' else x)

def get_total_rows(file_path):
    count = 0
    with open(file_path, 'r', encoding='utf-8') as file:
        for _ in file:
            count += 1
    return count

def download_dataset(logger):
    dataset_path = '/opt/airflow/dataset/arxiv.zip'

    if not os.path.exists(dataset_path):
        try:
            logger.info("Dataset did not exist, attempting to download")
            kaggle_api = KaggleApi()
            kaggle_api.authenticate()
            kaggle_api.dataset_download_files('Cornell-University/arxiv', path='/opt/airflow/dataset/', unzip=False)
        except Exception as e:
            logger.error(f"Failed to download dataset: {e}")
            raise

def unzip_dataset(logger):
    dataset_path = '/opt/airflow/dataset/arxiv.zip'
    extracted_path = '/opt/airflow/dataset/arxiv-metadata-oai-snapshot.json'

    if os.path.exists(dataset_path) and not os.path.exists(extracted_path):
        try:
            with zipfile.ZipFile(dataset_path, 'r') as zip_ref:
                zip_ref.extractall('/opt/airflow/dataset/')
        except zipfile.BadZipFile as e:
            logger.error(f"Failed to unzip dataset: {e}")
            raise

def clean_and_validate_dataset(logger):
    file_path = '/opt/airflow/dataset/arxiv-metadata-oai-snapshot.json'
    base_output_path = '/opt/airflow/staging_area/arxiv_transformed_part_'
    
    if not os.path.exists(file_path):
        logger.info(f"File {file_path} does not exist. Operation skipped.")
        return

    # total_rows = get_total_rows(file_path)
    total_rows = 10000
    rows_per_subset = total_rows // 4

    for part in range(4):
        # Load a subset of the dataset
        subset_start_row = part * rows_per_subset
        df = load_dataset(file_path, subset=True, start_row=subset_start_row, rows=rows_per_subset)
        
        # Process the DataFrame
        handle_id(df)
        handle_authors(df)
        map_general_categories(df, logger)
        # consume_crossref(df, logger)
        consume_semantic_scholar(df, logger)

        # Save the processed subset
        output_path = f"{base_output_path}{part}.json"
        df.to_json(output_path, orient='records', lines=True)
        logger.info(f"Subset {part} of DataFrame saved to {output_path}")