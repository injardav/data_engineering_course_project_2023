import uuid, json, pandas as pd

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
    category_mapping = load_category_mapping('/opt/airflow/data/category_mapping.json', logger)
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
