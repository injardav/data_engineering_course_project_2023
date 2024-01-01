import os, zipfile, uuid, json, pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

def load_dataset(file_path, logger, subset=False, start_row=0, rows=10):
    """
    Loads a specific subset of the dataset starting from `start_row` and 
    loading `rows` number of rows, or the entire dataset if `subset` is False.
    Applies dropna to remove rows where both 'doi' and 'arxiv' are NaN.
    Checks if the file exists before attempting to load.
    """
    logger.info(f"Attempting to load dataset from: {file_path}")

    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return pd.DataFrame()

    data = []
    current_row = 0

    with open(file_path, 'r') as file:
        for line in file:
            if subset and current_row >= start_row + rows:
                break

            row = json.loads(line)
            if subset:
                if ('doi' in row and row['doi']) and ('id' in row and row['id']): # arXiv value is under 'id' field
                    if current_row >= start_row:
                        data.append(row)
                    current_row += 1
            else:
                data.append(row)

    df = pd.DataFrame(data)

    if subset:
        logger.info(f"Loaded subset of data: start_row={start_row}, rows={rows}")
    else:
        df = df.dropna(subset=['arxiv', 'doi'], how='all')
        logger.info("Loaded entire dataset with NaN 'arxiv' and 'doi' dropped")

    logger.info(f"Total rows loaded: {len(df)}")
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

def handle_id(df, logger):
    """
    Rename `id` to `arxiv` and create correct `id` field with UUID values
    """
    logger.info("Renaming id column to arxiv. Creating new id column with uuid values")
    df.rename(columns={'id': 'arxiv'}, inplace=True)
    df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]

def get_total_rows(file_path):
    count = 0
    with open(file_path, 'r', encoding='utf-8') as file:
        for _ in file:
            count += 1
    return count

def download_dataset(kaggle_dataset, dataset_path, **kwargs):
    logger = kwargs['ti'].log
    logger.info("Starting download dataset process")

    if not os.path.exists(dataset_path):
        try:
            logger.info("Dataset did not exist, attempting to download")
            kaggle_api = KaggleApi()
            kaggle_api.authenticate()
            kaggle_api.dataset_download_files(kaggle_dataset, path='/opt/airflow/dataset/', unzip=False)
        except Exception as e:
            logger.error(f"Failed to download dataset: {e}")
            raise
    else:
        logger.info("Dataset exists, proceeding")

def unzip_dataset(dataset_path, extracted_path, **kwargs):
    logger = kwargs['ti'].log
    logger.info("Starting unzipping dataset process")

    if os.path.exists(dataset_path) and not os.path.exists(extracted_path):
        try:
            logger.info("Dataset not unzipped, attempting to unzip")
            with zipfile.ZipFile(dataset_path, 'r') as zip_ref:
                zip_ref.extractall('/opt/airflow/dataset/')
        except zipfile.BadZipFile as e:
            logger.error(f"Failed to unzip dataset: {e}")
            raise
    else:
        logger.info("Dataset either did not exist or was already unzipped, proceeding")

def delete_file(file_path, **kwargs):
    """
    Deletes a file at the given file_path.

    :param file_path: The full path of the file to be deleted.
    """
    logger = kwargs['ti'].log

    if os.path.exists(file_path):
        try:
            logger.info(f"Attempting to delete file: {file_path}")
            os.remove(file_path)
            logger.info(f"File successfully deleted: {file_path}")
        except Exception as e:
            logger.error(f"Failed to delete file: {e}")
            raise
    else:
        logger.info(f"File does not exist, nothing to delete: {file_path}")

def clean_and_validate_dataset(file_path, **kwargs):
    base_output_path = '/opt/airflow/staging_area/arxiv_preprocessed_part_'
    
    logger = kwargs['ti'].log
    logger.info("Starting cleaning and validating process")

    if not os.path.exists(file_path):
        logger.info(f"File {file_path} does not exist. Operation skipped.")
        return

    # total_rows = get_total_rows(file_path)
    total_rows = 10000
    total_parts = 4
    rows_per_subset = total_rows // total_parts

    for part in range(1, total_parts + 1):
        subset_start_row = (part - 1) * rows_per_subset

        logger.info(f"Processing subset starting from row {subset_start_row} with step {rows_per_subset}")
        df = load_dataset(file_path, logger, subset=True, start_row=subset_start_row, rows=rows_per_subset)
        
        # Process the DataFrame
        handle_id(df, logger)
        map_general_categories(df, logger)

        # Save the processed subset
        output_path = f"{base_output_path}{part}.json"
        df.to_json(output_path, orient='records', lines=True)
        logger.info(f"Subset {part} of DataFrame saved to {output_path}")
