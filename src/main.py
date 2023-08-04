import argparse
import contextlib
import os.path
import os.path as path
import time
import cProfile
import string
from tqdm import tqdm
from glob import glob
from loguru import logger
import pandas as pd
from random import choice, randint
from src.utils.metrics import MetricsHelper
from src.utils.helper import Helper

logger.add("logs/{time}.log")
MERGED_FILE = 'merged.parquet'
CSV_APPEND_STEPS = 100
helper = Helper()
metrics = MetricsHelper()


def benchmark_csv():
    """

    # Create a file of size 1MB
    for step in range(100):
        add line to each file
        push to cloud
    """
    appended_filepath = filepath = 'blog/file.csv'
    filename = path.basename(filepath)
    metrics.set_file(filepath=filepath,
                     step=0,
                     file_bytes=metrics.get_file_size(filepath),
                     is_merged=True)
    metrics.record(helper.copy_file, tech=Helper.M1, filepath=filepath, repo=helper.DVC)
    metrics.record(helper.copy_file, tech=Helper.M1, filepath=filepath, repo=helper.LFS_S3)
    metrics.record(helper.copy_file, tech=Helper.M1, filepath=filepath, repo=Helper.LFS_GITHUB)
    metrics.record(helper.copy_file, tech=Helper.M1, filepath=filepath, repo=Helper.XETHUB_GIT)

    original = pd.read_csv(filepath, nrows=650)
    genders = list(set(original['gender']))
    topics = list(set(original['topic']))
    signs = list(set(original['sign']))
    counter = original['id'].max()
    date = pd.to_datetime(original['date'].max())

    def generate_text():
        return ''.join([choice(string.ascii_letters) for _ in range(100)])

    def generate_row(counter: int, date: pd.Timestamp):
        new_row = {'id': counter,
                   'gender': choice(genders),
                   'age': randint(0, 100),
                   'topic': choice(topics),
                   'signs': choice(signs),
                   'date': date.strftime('%d,%B,%Y'),
                   'text': generate_text()
                   }

        yield pd.DataFrame([new_row]), counter + 1, date + pd.DateOffset(hours=1)

    def append_row(row, filepath):
        row.to_csv(filepath, mode='a', header=False, index=False)

    for step in range(CSV_APPEND_STEPS):
        metrics.set_file(filepath=filepath,
                         step=step,
                         file_bytes=metrics.get_file_size(filepath),
                         is_merged=True)
        if step > 0:
            row, counter, date = next(generate_row(counter, date))
            for repo in [helper.DVC, helper.LFS_S3, Helper.LFS_GITHUB, Helper.XETHUB_GIT]:
                appended_filepath = os.path.join(repo, filename)
                append_row(row, appended_filepath)
        metrics.record(helper.xethub_git_merged_upload, tech=Helper.XETHUB, filepath=filepath)
        metrics.record(helper.dvc_merged_upload, tech=Helper.DVC, filepath=filepath)
        metrics.record(helper.lfs_s3_merged_upload, tech=Helper.LFS, filepath=filepath)
        metrics.record(helper.s3_merged_upload, tech=Helper.S3, filepath=filepath)
        metrics.record(helper.lfs_git_merged_upload, tech=Helper.LFS, filepath=filepath)
        metrics.record(helper.xethub_py_merged_upload, tech=Helper.XETHUB, filepath=appended_filepath)
        metrics.record(helper.lakefs_merged_upload, tech=Helper.LAKEFS, filepath=appended_filepath)
        metrics.export()


def benchmark_files(data: str):
    files = sorted(glob(f"{data}/*.parquet"))
    logger.info(f"benchmark- {data} : {len(files)} files")
    merged_filepath = path.join(data, 'merged.parquet')
    file_count = len(files)
    if merged_filepath in files:
        file_count -= 1

    for step, filepath in tqdm(enumerate(files)):
        metrics.set_file(filepath=filepath,
                         step=step,
                         file_bytes=metrics.get_file_size(filepath),
                         is_merged=False)
        # Create merged file
        metrics.record(helper.merge_files, tech=Helper.M1, new_filepath=filepath, merged_filepath=merged_filepath)
        # copy locally
        metrics.record(helper.copy_file, tech=Helper.M1, filepath=filepath, repo=helper.DVC)
        metrics.record(helper.copy_file, tech=Helper.M1, filepath=merged_filepath, repo=helper.DVC)
        metrics.record(helper.copy_file, tech=Helper.M1, filepath=filepath, repo=helper.LFS_S3)
        metrics.record(helper.copy_file, tech=Helper.M1, filepath=merged_filepath, repo=helper.LFS_S3)
        metrics.record(helper.copy_file, tech=Helper.M1, filepath=filepath, repo=Helper.LFS_GITHUB)
        metrics.record(helper.copy_file, tech=Helper.M1, filepath=merged_filepath, repo=Helper.LFS_GITHUB)
        metrics.record(helper.copy_file, tech=Helper.M1, filepath=filepath, repo=Helper.XETHUB_GIT)
        metrics.record(helper.copy_file, tech=Helper.M1, filepath=merged_filepath, repo=Helper.XETHUB_GIT)

        metrics.record(helper.xethub_py_new_upload, tech=Helper.XETHUB, filepath=filepath)
        metrics.record(helper.dvc_new_upload, tech=Helper.DVC, filepath=filepath)
        metrics.record(helper.lfs_s3_new_upload, tech=Helper.LFS, filepath=filepath)
        metrics.record(helper.xethub_git_new_upload, tech=Helper.XETHUB, filepath=filepath)
        metrics.record(helper.lfs_git_new_upload, tech=Helper.LFS, filepath=filepath)
        metrics.record(helper.s3_new_upload, tech=Helper.S3, filepath=filepath)
        metrics.record(helper.lakefs_new_upload, tech=Helper.LAKEFS, filepath=filepath)

        metrics.set_file(filepath=filepath, step=step, file_bytes=metrics.get_file_size(merged_filepath),
                         is_merged=True)
        metrics.record(helper.xethub_py_merged_upload, tech=Helper.XETHUB, filepath=merged_filepath)
        metrics.record(helper.dvc_merged_upload, tech=Helper.DVC, filepath=merged_filepath)
        metrics.record(helper.lfs_s3_merged_upload, tech=Helper.LFS, filepath=merged_filepath)
        metrics.record(helper.xethub_git_merged_upload, tech=Helper.XETHUB, filepath=merged_filepath)
        metrics.record(helper.lfs_git_merged_upload, tech=Helper.LFS, filepath=merged_filepath)
        metrics.record(helper.s3_merged_upload, tech=Helper.S3, filepath=merged_filepath)
        metrics.record(helper.lakefs_merged_upload, tech=Helper.LAKEFS, filepath=merged_filepath)

        metrics.export()  # TODO write just the last row


if __name__ == '__main__':
    p = argparse.ArgumentParser('Benchmarking of NYC Taxi data in different repositories')
    p.add_argument('-d',
                   '--dir', default='mock',
                   help='The directory in which to download data and perform searches.')
    p.add_argument('-u',
                   '--upload', default=False, action='store_true',
                   help='If True, upload to repo')
    p.add_argument('-s',
                   '--show', default=False, action='store_true',
                   help='If True, run snakeviz server')
    args = p.parse_args()
    metrics.data_dir = args.dir
    metrics.file_count = len(glob(f"{args.dir}/*"))
    profiler = cProfile.Profile()
    start = time.time()
    command = f"benchmark_files('{args.dir}')"
    if args.dir == 'blog':
        command = f"benchmark_csv()"
    profiler.run(command)
    profiler.dump_stats('output/profile.prof')

    if args.upload:
        with contextlib.suppress(Exception):  # Not to break the flow
            helper.output_upload(f"Experiment {metrics.datetime} - data: {args.dir}")
    print(f"######### Total time: {time.time() - start} #########")
    if args.show:
        with contextlib.suppress(Exception):  # Not to break the flow
            print(helper.run("snakeviz output/profile.prof"))
