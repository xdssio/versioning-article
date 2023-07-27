import os
import os.path as path
import pathlib
import shutil
import psutil
import typing
from datetime import datetime as dt
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import tqdm
import duckdb
import time

NYC_TLC_SITE = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
HFVHFV_PATTERN = r'fhvhv_tripdata_'
DOWNLOAD_CHOICES = ['all', '2023', '2022', '2021', '2020', '2019']
MERGED_FILENAME = 'merged.parquet'
MOCK_COLUMNS = ['id', 'name', 'age']


def generate_parquet(num_rows: int = 10):
    import random
    import pandas as pd

    rows = []
    for i in range(num_rows):
        rows.append({
            'id': i + 1,
            'name': f'Person {i + 1}',
            'age': random.randint(18, 1e6)
        })
    return pd.DataFrame(rows, columns=MOCK_COLUMNS)


def save_parquet(df: pd.DataFrame, filename: str):
    df.to_parquet(filename)


def generate_mock_data(target: str, file_count: int = 5, num_rows: int = 10):
    """for development """
    shutil.rmtree(target, ignore_errors=True)
    os.mkdir(target)

    for filename in tqdm.tqdm(range(0, file_count)):
        save_parquet(generate_parquet(num_rows), f"{target}/{filename}.parquet")


def merge_files(new_filename:str, merged_filename: str):
    if not path.exists(merged_filename):
        shutil.copyfile(new_filename, merged_filename)
    else:
        duckdb.execute(f"""
                    COPY (SELECT * FROM read_parquet(['{new_filename}', '{merged_filename}'])) TO '{merged_filename}' (FORMAT 'parquet');
                    """)
    return merged_filename








