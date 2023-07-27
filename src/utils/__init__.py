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


def merge(merged_filename: str, new_filename: str):
    if not path.exists(merged_filename):
        shutil.copyfile(new_filename, merged_filename)
    else:
        duckdb.execute(f"""
                    COPY (SELECT * FROM read_parquet(['{new_filename}', '{merged_filename}'])) TO '{merged_filename}' (FORMAT 'parquet');
                    """)


def download(download_dir: str, choices: str, limit: int):
    '''
    Extract URLs which match the patterns and download them into download_dir
    :param download_dir:
        The directory to download the data
    :param choices:
        Year to download or 'all'
    '''
    print("download_data")
    if path.exists(download_dir) and not path.isdir(download_dir):
        raise RuntimeError("Cannot download into " + download_dir)
    if not path.exists(download_dir):
        os.mkdir(download_dir)
    response = requests.get(NYC_TLC_SITE)
    soup = BeautifulSoup(response.content, 'html.parser')

    anchor_tags = soup.find_all('a', href=True)

    if choices == 'all':
        pattern = re.compile(HFVHFV_PATTERN)
    else:
        pattern = re.compile(HFVHFV_PATTERN + choices)
    matching_urls = [tag['href'] for tag in anchor_tags if pattern.search(tag['href'])]
    download_count = 0

    for url in tqdm.tqdm(matching_urls):
        download_count += 1
        if download_count >= limit > 0:
            break
        filename = path.join(download_dir, url.split('/')[-1])
        if not path.exists(filename):
            print(f"Downloading {filename}")
            response = requests.get(url)
            # Extract the filename from the URL
            with open(filename, 'wb') as file:
                file.write(response.content)


class MetricsHelper:

    def __init__(self, data_dir: str, file_count: int):
        self.data_dir = data_dir
        self.file_count = file_count
        self.datetime = dt.now().strftime("%d/%m/%Y %H:%M:%S")
        self.steps = []
        self.filename = None
        self.step = -1

    def set_file(self, filename: str):
        self.filename = filename
        # TODO add file bytes size
        self.step += 1

    def bytes_in_rest(self, duration: float):
        net_io_before_sleep = psutil.net_io_counters()
        time.sleep(duration)
        net_io_after_sleep = psutil.net_io_counters()
        sleep_bytes_sent = net_io_after_sleep.bytes_sent - net_io_before_sleep.bytes_sent
        sleep_bytes_recv = net_io_after_sleep.bytes_recv - net_io_before_sleep.bytes_recv
        return sleep_bytes_sent, sleep_bytes_recv

    def record(self, func: typing.Callable, tech: str, merged: bool = True, *args, **kwargs, ):
        start_func_time = time.time()
        net_io_before = psutil.net_io_counters()
        func(*args, **kwargs)
        net_io_after = psutil.net_io_counters()
        func_time = time.time() - start_func_time
        bytes_sent = net_io_after.bytes_sent - net_io_before.bytes_sent
        bytes_recv = net_io_after.bytes_recv - net_io_before.bytes_recv
        sleep_bytes_sent, sleep_bytes_recv = self.bytes_in_rest(1)

        self.steps.append({'time': func_time,
                           'tech': tech,
                           'merged': merged,
                           'filename': self.filename,
                           'step': self.step,
                           'bytes_sent': bytes_sent,
                           'bytes_recv': bytes_recv,
                           'bytes_sent_1s': sleep_bytes_sent,
                           'bytes_recv_1s': sleep_bytes_recv
                           })

    def track(self, duration: float, tech: str, merged: bool = True):
        self.steps.append(
            {'time': duration,
             'tech': tech,
             'merged': merged,
             'filename': self.file,
             'step': self.step})

    def _get_output(self):
        df = pd.DataFrame(self.steps)
        df['data_dir'] = self.data_dir
        df['file_count'] = self.file_count
        df['datetime'] = self.datetime
        return df

    def export(self, output: str = 'output.csv'):
        pathlib.Path(output).parent.mkdir(parents=True, exist_ok=True)
        df = self._get_output()
        df.to_csv(output, index=False)


class GitHelper():
    pass