import argparse
import os
import os.path as path
import subprocess
import shutil
import time
import duckdb
from tqdm import tqdm
from glob import glob
from loguru import logger
from src.utils.metrics import MetricsHelper
from src.utils.git import GitHelper


logger.add("logs/{time}.log")
logger.debug("That's it, beautiful and simple logging!")

NYC_TLC_SITE = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
HFVHFV_PATTERN = r'fhvhv_tripdata_'
DOWNLOAD_CHOICES = ['all', '2023', '2022', '2021', '2020', '2019']
MERGED_FILENAME = 'merged.parquet'

sleep_time = 0.5



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


def benchmark(data_dir, output_file):
    files = list(glob(f"{data_dir}/*.parquet"))
    print(f"benchmark- {data_dir} : {len(files)} files")

    metrics = MetricsHelper(data_dir=data_dir, file_count=len(files))
    for file in tqdm(files):
        metrics.set_file(file)
        # Create merged file
        metrics.record(upload_dvc, tech='dvc', merged=False)
        metrics.record(upload_dvc_merged, tech='dvc', merged=True)
        metrics.record(upload_lfs, tech='lfs', merged=False)
        metrics.record(upload_lfs_merged, tech='lfs', merged=True)
        metrics.record(upload_xethub, tech='xethub', merged=False)
        metrics.record(upload_xethub_merged, tech='xethub', merged=True)

    metrics.export(output_file)


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
    p.add_argument(
        '--output', default='output.csv',
        help='The the location for the csv output file.')

    args = p.parse_args()
    benchmark(args.dir, args.output)
