#!/usr/bin/env python3

import argparse
import os
import os.path as path
import subprocess
import shutil
import time
import duckdb
from tqdm import tqdm
from glob import glob
from aim import Run

NYC_TLC_SITE = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
HFVHFV_PATTERN = r'fhvhv_tripdata_'
DOWNLOAD_CHOICES = ['all', '2023', '2022', '2021', '2020', '2019']
MERGED_FILENAME = 'merged.parquet'


def benchmark(data_dir):
    files = list(glob(f"{data_dir}/.parquet"))
    run = Run()
    run["hparams"] = {
        "learning_rate": 0.001,
        "batch_size": 32,
    }
    run["data_dir"] = data_dir
    run['file_count'] = len(files)

    for i, file in tqdm(enumerate(files)):
        run.track(i, name='loss', step=i, context={"subset": "train"})
        time.sleep(1)
        # upload_dvc()
        # upload_dvc_merged()


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


def main():
    p = argparse.ArgumentParser('Benchmarking of NYC Taxi data in different repositories')
    p.add_argument(
        '--dir', default='mock',
        help='The directory in which to download data and perform searches.')

    args = p.parse_args()
    benchmark(args.dir)


if __name__ == '__main__':
    main()
