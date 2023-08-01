import os
import os.path as path
import shutil
import pandas as pd
import random
import numpy as np
import tqdm
import duckdb

NYC_TLC_SITE = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
HFVHFV_PATTERN = r'fhvhv_tripdata_'
DOWNLOAD_CHOICES = ['all', '2023', '2022', '2021', '2020', '2019']
MERGED_FILENAME = 'merged.parquet'


def generate_parquet(num_rows: int = 10):
    rows = []
    for i in range(num_rows):
        rows.append({
            'id': i + 1,
            'name': f'Person {i + 1}',
            'age': random.randint(18, 100),
            'salary': np.random.randint(10000, 100000),
            'city': 'New York'
        })
    df = pd.DataFrame(rows)
    # df['date'] = pd.date_range(start='1900-01-01', periods=num_rows, freq='H')
    return df


def save_parquet(df: pd.DataFrame, filename: str):
    df.to_parquet(filename)


def generate_mock_data(target: str, file_count: int = 5, num_rows: int = 10):
    """for development """
    shutil.rmtree(target, ignore_errors=True)
    os.mkdir(target)

    for filename in tqdm.tqdm(range(0, file_count)):
        save_parquet(generate_parquet(num_rows), f"{target}/{filename}.parquet")


def merge_files(new_filename: str, merged_filename: str):
    if not path.exists(merged_filename):
        shutil.copyfile(new_filename, merged_filename)
    else:
        duckdb.execute(f"""
                    COPY (SELECT * FROM read_parquet(['{new_filename}', '{merged_filename}'])) TO '{merged_filename}' (FORMAT 'parquet');
                    """)
    return merged_filename
