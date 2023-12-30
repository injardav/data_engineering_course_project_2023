import uuid, json, pandas as pd

def load_dataset(file_path, subset=False, rows=10):
    """
    Loads either entire dataset or if subset is True, then loads `rows` subset only.
    """
    if subset:
        rows_with_doi = []
        with open(file_path, 'r') as file:
            for line in file:
                row = json.loads(line)
                if ('doi' in row and row['doi']) and ('id' in row and row['id']):
                    rows_with_doi.append(row)
                    if len(rows_with_doi) == rows:
                        break
        return pd.DataFrame(rows_with_doi)
    
    df = pd.read_json(file_path, lines=True)
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
    category_mapping = load_category_mapping('/opt/airflow/data/category_mapping.json', logger)
    df['general_category'] = df['categories'].apply(lambda x: map_category(x, category_mapping, logger))
    df.drop('categories', axis=1, inplace=True)

def handle_id(df):
    """
    Rename `id` to `arxiv` and create correct `id` field with UUID values
    """
    df.rename(columns={'id': 'arxiv'}, inplace=True)
    df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]