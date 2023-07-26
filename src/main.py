import argparse
import os
import os.path as path
import subprocess
import shutil
import time
import duckdb
from tqdm import tqdm
from glob import glob
from collections import namedtuple

from src.utils import RunHelper

NYC_TLC_SITE = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
HFVHFV_PATTERN = r'fhvhv_tripdata_'
DOWNLOAD_CHOICES = ['all', '2023', '2022', '2021', '2020', '2019']
MERGED_FILENAME = 'merged.parquet'

RunInfo = namedtuple('RunInfo', ['run', 'step', 'file'])

sleep_time = 0.5





def upload_dvc(helper: RunHelper):
    start_time = time.time()
    """Do stuff"""
    time.sleep(sleep_time)
    helper.track(time.time() - start_time, tech='dvc', merged=False)


def upload_lfs(helper):
    start_time = time.time()
    """Do stuff"""
    time.sleep(sleep_time)
    helper.track(time.time() - start_time, tech='lfs', merged=False)


def upload_xethub(helper):
    start_time = time.time()
    """Do stuff"""
    time.sleep(sleep_time)
    helper.track(time.time() - start_time, tech='xethub', merged=False)


def upload_dvc_merged(helper):
    start_time = time.time()
    """Do stuff"""
    time.sleep(sleep_time)
    helper.track(time.time() - start_time, tech='dvc', merged=True)


def upload_lfs_merged(helper):
    start_time = time.time()
    """Do stuff"""
    time.sleep(sleep_time)
    helper.track(time.time() - start_time, tech='lfs', merged=True)


def upload_xethub_merged(helper):
    start_time = time.time()
    """Do stuff"""
    time.sleep(sleep_time)
    helper.track(time.time() - start_time, tech='xethub', merged=True)


def benchmark(data_dir):
    files = list(glob(f"{data_dir}/*.parquet"))
    print(f"benchmark- {data_dir} : {len(files)} files")

    helper = RunHelper(data_dir=data_dir, file_count=len(files))
    for file in tqdm(files):
        helper.set_file(file)
        # Create merged file
        upload_dvc(helper)
        upload_dvc_merged(helper)
        upload_lfs(helper)
        upload_lfs_merged(helper)
        upload_xethub(helper)
        upload_xethub_merged(helper)
    helper.export(f"output.csv")


def run_dvc_benchmark(repo: str = 'dvc', data: str = 'mock'):
    if path.exists(repo):
        shutil.rmtree(repo)
    os.mkdir(repo)

    commit_count = 1
    merged_filename = path.join(repo, MERGED_FILENAME)
    total_duration = 0

    for downloaded_file in glob(f"{data}/*.parquet"):
        if commit_count == 1:
            shutil.copyfile(downloaded_file, merged_filename)
            start = time.time()
            command = f"""
            dvc add {MERGED_FILENAME}
            git add {MERGED_FILENAME}.dvc .gitignore
            git commit -m "commit {commit_count}"
            """
            print(command)
            print(subprocess.run(command, capture_output=True, shell=True, cwd=repo))

            command = """
            dvc push
            """
            print(command)
            print(subprocess.run(command, capture_output=True, shell=True, cwd=repo))
            end = time.time()
            total_duration += (end - start)
        else:
            duckdb.execute(f"""
            COPY (SELECT * FROM read_parquet(['{downloaded_file}', '{merged_filename}'])) TO '{merged_filename}' (FORMAT 'parquet');
            """)
            start = time.time()
            command = f"""
            dvc add {MERGED_FILENAME}
            git add {MERGED_FILENAME}.dvc
            git commit -m "commit {commit_count}"
            """
            print(subprocess.run(command, capture_output=True, shell=True, cwd=args.dvc_dir))
            command = """
            dvc push
            """
            print(command)
            subprocess.run(command, capture_output=True, shell=True, cwd=args.dvc_dir)
            end = time.time()
            total_duration += (end - start)

        commit_count += 1
    print(f"Time taken for DVC for {commit_count} commits is {total_duration:.2f} seconds")


if __name__ == '__main__':
    p = argparse.ArgumentParser('Benchmarking of NYC Taxi data in different repositories')
    p.add_argument(
        '--dir', default='mock',
        help='The directory in which to download data and perform searches.')

    args = p.parse_args()
    benchmark(args.dir)
