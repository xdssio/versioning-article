import argparse
import os
import os.path as path
import requests
from bs4 import BeautifulSoup
import re
import tqdm

from utils import DOWNLOAD_CHOICES

NYC_TLC_SITE = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
HFVHFV_PATTERN = r'fhvhv_tripdata_'
DOWNLOAD_CHOICES = ['all', '2023', '2022', '2021', '2020', '2019']
MERGED_FILENAME = 'merged.parquet'
MOCK_COLUMNS = ['id', 'name', 'age']


def download(download_dir: str, choices: str, limit: int = -1):
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


if __name__ == '__main__':
    p = argparse.ArgumentParser('Benchmarking of NYC Taxi data in different repositories')
    p.add_argument(
        '--dir', default='data',
        help='The directory in which to download data and perform searches.')
    p.add_argument(
        '--download', default='all',
        choices=DOWNLOAD_CHOICES,
        help='Download data from these years'
             'Choices: {}'.format(', '.join(DOWNLOAD_CHOICES)))
    p.add_argument(
        '--limit', default=-1, type=int,
        help='max number of files to download')
    args = p.parse_args()
    download(args.dir, args.download, args.limit)
