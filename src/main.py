import argparse
import os.path as path
import time
import cProfile
from tqdm import tqdm
from glob import glob
from loguru import logger

from src.utils.metrics import MetricsHelper
from src.utils.helper import Helper

logger.add("logs/{time}.log")

helper = Helper()
metrics = MetricsHelper()


def benchmark(data: str):
    files = sorted(glob(f"{data}/*.parquet"))
    logger.info(f"benchmark- {data} : {len(files)} files")
    merged_filepath = path.join(data, 'merged.parquet')
    file_count = len(files)
    if merged_filepath in files:
        file_count -= 1

    for step, filepath in tqdm(enumerate(files)):
        metrics.set_file(filepath=filepath, step=step + 1)
        # Create merged file
        metrics.record(helper.merge_files, tech='m1', merged=True, new_filepath=filepath,
                       merged_filepath=merged_filepath)
        # copy locally
        metrics.record(helper.copy_file, tech='m1', merged=False, filepath=filepath, repo='dvc')
        metrics.record(helper.copy_file, tech='m1', merged=True, filepath=merged_filepath, repo='dvc')
        metrics.record(helper.copy_file, tech='m1', merged=False, filepath=filepath, repo='lfs')
        metrics.record(helper.copy_file, tech='m1', merged=True, filepath=merged_filepath, repo='lfs')

        metrics.record(helper.xethub_upload, tech='xethub', merged=False, filepath=filepath)
        metrics.record(helper.dvc_upload, tech='dvc', merged=False, filepath=filepath)
        metrics.record(helper.lfs_upload, tech='lfs', merged=False, filepath=filepath)

        metrics.record(helper.xethub_upload, tech='xethub', merged=True, filepath=merged_filepath)
        metrics.record(helper.dvc_upload, tech='dvc', merged=True, filepath=merged_filepath)
        metrics.record(helper.lfs_upload, tech='lfs', merged=True, filepath=merged_filepath)
        metrics.export()  # TODO write just the last row


if __name__ == '__main__':
    p = argparse.ArgumentParser('Benchmarking of NYC Taxi data in different repositories')
    p.add_argument(
        '--dir', default='mock',
        help='The directory in which to download data and perform searches.')
    args = p.parse_args()
    metrics.data_dir = args.dir
    metrics.file_count = len(glob(f"{args.dir}/*.parquet"))
    profiler = cProfile.Profile()
    start = time.time()
    profiler.run(f"benchmark('{args.dir}')")
    profiler.dump_stats('output/profile.prof')
    helper.output_upload(f"experiment {metrics._id} on data {args.dir}")

    print(f"######### Total time: {time.time() - start} #########")
    helper.run("snakeviz output/profile.prof")
