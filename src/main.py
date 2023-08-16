import argparse
import contextlib
import os.path
import os.path as path
import time
import shutil
import cProfile
from tqdm import tqdm
from glob import glob
from loguru import logger
import pandas as pd
from src.utils.metrics import MetricsHelper
from src.utils.helper import Helper
from src.utils import BlogRowsGenerator, generate_data

logger.add("logs/{time}.log")
helper = Helper()
metrics = MetricsHelper()

def benchmark_random(iterations: int = 100):
    logger.info(f"benchmark - random - {iterations} files")
    stop = False
    for step in tqdm(range(iterations)):
        filename = f"{step}.csv"
        filepath = f"random/{filename}"
        df = generate_data(num_rows=1000)
        df.to_csv(filepath)
        stop = False  # for graceful exit
        for step in range(iterations):
            if stop:
                break

            metrics.set_file(filepath=filepath,
                             step=step,
                             file_bytes=metrics.get_file_size(filepath),
                             is_merged=False)
            for repo in [helper.DVC, helper.LFS_S3, Helper.LFS_GITHUB, Helper.XETHUB_GIT]:
                new_filepath = os.path.join(repo, filename)
                df.to_csv(new_filepath, index=False)
            for func, tech in [(helper.xethub_git_new_upload, Helper.XETHUB),
                               (helper.dvc_new_upload, Helper.DVC),
                               (helper.lfs_s3_new_upload, Helper.LFS),
                               (helper.s3_new_upload, Helper.S3),
                               (helper.lfs_git_new_upload, Helper.LFS),
                               (helper.xethub_py_new_upload, Helper.XETHUB),
                               (helper.lakefs_new_upload, Helper.LAKEFS)]:
                stop = stop or metrics.record(func, tech=tech, filepath=filepath)
                metrics.export()

def benchmark_blog(iterations: int = 100):
    logger.info(f"benchmark - blog - {iterations} iterations")
    original = 'blog/original.csv'
    appended_filepath = filepath = 'blog/blog.csv'

    shutil.copyfile(original, filepath)
    filename = path.basename(filepath)
    metrics.set_file(filepath=filepath,
                     step=0,
                     file_bytes=metrics.get_file_size(filepath),
                     is_merged=True)
    metrics.record(helper.copy_file, tech=Helper.M1, filepath=filepath, repo=helper.DVC)
    metrics.record(helper.copy_file, tech=Helper.M1, filepath=filepath, repo=helper.LFS_S3)
    metrics.record(helper.copy_file, tech=Helper.M1, filepath=filepath, repo=Helper.LFS_GITHUB)
    metrics.record(helper.copy_file, tech=Helper.M1, filepath=filepath, repo=Helper.XETHUB_GIT)
    data = pd.read_csv(filepath, nrows=68200)
    generator = BlogRowsGenerator(counter=data['id'].max(),
                                  date=data['date'].max(),
                                  genders=list(set(data['gender'])),
                                  topics=list(set(data['topic'])),
                                  signs=list(set(data['sign'])),
                                  )
    stop = False  # for graceful exit
    for step in range(iterations):
        if stop:
            break
        try:
            metrics.set_file(filepath=filepath,
                             step=step,
                             file_bytes=metrics.get_file_size(filepath),
                             is_merged=True)
            if step > 0:
                for repo in [helper.DVC, helper.LFS_S3, Helper.LFS_GITHUB, Helper.XETHUB_GIT]:
                    appended_filepath = os.path.join(repo, filename)
                    generator.append_mock_row(appended_filepath)
            for func, tech in [(helper.xethub_git_new_upload, Helper.XETHUB),
                               (helper.dvc_new_upload, Helper.DVC),
                               (helper.lfs_s3_new_upload, Helper.LFS),
                               (helper.s3_new_upload, Helper.S3),
                               (helper.lfs_git_new_upload, Helper.LFS),
                               (helper.xethub_py_new_upload, Helper.XETHUB),
                               (helper.lakefs_new_upload, Helper.LAKEFS)]:
                stop = stop or metrics.record(func, tech=tech, filepath=filepath)
                metrics.export()
        except KeyboardInterrupt as e:
            logger.info(f"KeyboardInterrupt: {e}")
            break


def benchmark_taxi(iterations: int = 100):
    files = sorted(glob(f"taxi/*.parquet"))
    logger.info(f"benchmark - taxi : {len(files)} files")
    merged_filepath = path.join('taxi', 'merged.parquet')
    file_count = len(files)
    if merged_filepath in files:
        file_count -= 1

    for step, filepath in tqdm(enumerate(files)):
        if step == iterations:
            break
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
    p.add_argument('experiment', choices=['blog', 'taxi', 'random'], help='The experiment to run')
    p.add_argument('-i', '--iterations', type=int, help='Number of iterations to run', default=10)
    p.add_argument('-u',
                   '--upload', default=False, action='store_true',
                   help='If True, upload to repo')
    p.add_argument('-s',
                   '--show', default=False, action='store_true',
                   help='If True, run snakeviz server')
    args = p.parse_args()
    iterations = args.iterations
    if args.experiment == 'blog':
        data_dir = 'blog'
        command = f"benchmark_blog({iterations})"
    elif args.experiment == 'taxi':
        data_dir = 'taxi'
        command = f"benchmark_taxi({iterations})"
    elif args.experiment == 'random':
        data_dir = 'random'
        command = f"benchmark_random({iterations})"
    else:
        # not implemented
        data_dir = 'mock'
        command = ""
    metrics.data_dir = data_dir
    metrics.file_count = len(glob(f"{data_dir}/*"))
    profiler = cProfile.Profile()
    start = time.time()
    profiler.run(command)
    profiler.dump_stats('output/profile.prof')

    if args.upload:
        with contextlib.suppress(Exception):  # Not to break the flow
            helper.output_upload(f"Experiment {metrics.datetime} - data: {args.dir}")
    print(f"######### Total time: {time.time() - start} #########")
    if args.show:
        with contextlib.suppress(Exception):  # Not to break the flow
            print(helper.run("snakeviz output/profile.prof"))
