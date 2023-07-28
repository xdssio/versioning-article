import argparse
import os.path as path
import time

from tqdm import tqdm
from glob import glob
from loguru import logger

from src.utils.metrics import MetricsHelper
from src.utils.helper import Helper

logger.add("logs/{time}.log")

helper = Helper()


def benchmark(data: str, output_file: str = 'output.csv'):
    files = sorted(glob(f"{data}/*.parquet"))
    logger.info(f"benchmark- {data} : {len(files)} files")
    merged_filepath = path.join(data, 'merged.parquet')
    file_count = len(files)
    if merged_filepath in files:
        file_count -= 1

    metrics = MetricsHelper(data_dir=data, file_count=file_count)

    for step, filepath in tqdm(enumerate(files)):
        metrics.set_file(filepath=filepath, step=step)
        # Create merged file
        metrics.record(helper.merge_files, tech='m1', merged=True, new_filename=filepath,
                       merged_filename=merged_filepath)
        # copy locally
        metrics.record(helper.copy_file, tech='m1', merged=False, filepath=filepath, repo='dvc')
        metrics.record(helper.copy_file, tech='m1', merged=True, filepath=merged_filepath, repo='dvc')
        metrics.record(helper.copy_file, tech='m1', merged=False, filepath=filepath, repo='lfs')
        metrics.record(helper.copy_file, tech='m1', merged=True, filepath=merged_filepath, repo='lfs')

        metrics.record(helper.xethub_upload, tech='xethub', merged=False, filepath=filepath)
        metrics.record(helper.dvc_upload, tech='dvc', merged=False, filepath=filepath)
        metrics.record(helper.dvc_upload, tech='dvc', merged=True, filepath=merged_filepath)
        metrics.record(helper.lfs_upload, tech='lfs', merged=False, filepath=filepath)
        metrics.record(helper.lfs_upload, tech='lfs', merged=True, filepath=merged_filepath)
        metrics.record(helper.xethub_upload, tech='xethub', merged=True, filepath=merged_filepath)
        metrics.export(output_file)  # TODO write just the last row


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
