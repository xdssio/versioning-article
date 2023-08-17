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
from src.utils.generators import BlogRowsGenerator, DataFrameGenerator
from xetrack import Tracker

logger.add("logs/{time}.log")
helper = Helper()
metrics = MetricsHelper()
tracker = Tracker('output/stats.db', verbose=False)


def benchmark_random(iterations: int = 100):
    logger.info(f"benchmark - random - {iterations} files")
    stop = False  # for graceful exit
    filename = f"data.parquet"
    filepath = f"random/{filename}"
    generator = DataFrameGenerator(num_rows=10000)  # ~ 1.2MB
    for step in tqdm(range(iterations)):
        if stop:
            break
        df = generator.generate_data()
        df.to_parquet(filepath)
        metrics.set_file(filepath=filepath,
                         step=step,
                         file_bytes=metrics.get_file_size(filepath),
                         is_merged=False)
        for repo in [helper.DVC, helper.LFS_S3, Helper.LFS_GITHUB, Helper.XETHUB_GIT]:
            new_filepath = os.path.join(repo, filename)
            df.to_parquet(new_filepath, index=False)
        for func, tech in [(helper.gitxet_new_upload, Helper.XETHUB),
                           (helper.dvc_new_upload, Helper.DVC),
                           (helper.lfs_s3_new_upload, Helper.LFS),
                           (helper.s3_new_upload, Helper.S3),
                           (helper.lfs_git_new_upload, Helper.LFS),
                           (helper.pyxet_new_upload, Helper.XETHUB),
                           (helper.lakefs_new_upload, Helper.LAKEFS)]:
            stop = stop or metrics.record(func, tech=tech, filepath=filepath)
            metrics.export()


def benchmark_blog(iterations: int = 100):
    logger.info(f"benchmark - blog - {iterations} iterations")
    original = 'blog/original.csv'
    appended_filepath = filepath = 'blog/blog.csv'

    data = pd.read_csv(original, nrows=68200)
    generator = BlogRowsGenerator(counter=data['id'].max(),
                                  date=data['date'].max(),
                                  genders=list(set(data['gender'])),
                                  topics=list(set(data['topic'])),
                                  signs=list(set(data['sign'])),
                                  )
    shutil.copyfile(original, filepath)
    filename = path.basename(filepath)
    tracker.set_params({'merged': True, 'step': -1, 'filename': filename, 'tech': Helper.M1})
    for repo in [helper.DVC, helper.LFS_S3, Helper.LFS_GITHUB, Helper.XETHUB_GIT]:
        tracker.track_function(helper.copy_file, filepath=filepath, repo=repo)

    stop = False  # for graceful exit
    for step in range(iterations):
        if stop:
            break
        try:
            tracker.set_params({'filename': filename,
                                'step': step,
                                'file_bytes': helper.get_file_size(filepath),
                                'is_merged': True})

            for repo in [helper.DVC, helper.LFS_S3, Helper.LFS_GITHUB, Helper.XETHUB_GIT]:
                generator.append_mock_row(os.path.join(repo, filename))
            for func in (helper.gitxet_merged_upload,
                         helper.dvc_merged_upload,
                         helper.lfs_s3_merged_upload,
                         helper.s3_merged_upload,
                         helper.lfs_git_merged_upload,
                         helper.pyxet_merged_upload,
                         helper.lakefs_merged_upload):
                try:
                    tracker.track_function(func, filepath=filepath)
                except KeyboardInterrupt:
                    stop = True
        except KeyboardInterrupt as e:
            logger.info(f"KeyboardInterrupt: {e}")
            break


def benchmark_taxi(iterations: int = 20):
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
        for repo in [helper.DVC, helper.LFS_S3, Helper.LFS_GITHUB, Helper.XETHUB_GIT]:
            metrics.record(helper.copy_file, tech=Helper.M1, filepath=filepath, repo=repo)
            metrics.record(helper.copy_file, tech=Helper.M1, filepath=merged_filepath, repo=repo)

        for func, tech in [(helper.gitxet_new_upload, Helper.XETHUB),
                           (helper.dvc_new_upload, Helper.DVC),
                           (helper.lfs_s3_new_upload, Helper.LFS),
                           (helper.s3_new_upload, Helper.S3),
                           (helper.lfs_git_new_upload, Helper.LFS),
                           (helper.pyxet_new_upload, Helper.XETHUB),
                           (helper.lakefs_new_upload, Helper.LAKEFS)]:
            metrics.record(func, tech=tech, filepath=filepath)
            metrics.export()

        metrics.set_file(filepath=merged_filepath, step=step, file_bytes=metrics.get_file_size(merged_filepath),
                         is_merged=True)
        for func, tech in [(helper.pyxet_merged_upload, Helper.XETHUB),
                           (helper.dvc_merged_upload, Helper.DVC),
                           (helper.lfs_s3_merged_upload, Helper.LFS),
                           (helper.gitxet_merged_upload, Helper.XETHUB),
                           (helper.lfs_git_merged_upload, Helper.LFS),
                           (helper.s3_merged_upload, Helper.S3),
                           (helper.lakefs_merged_upload, Helper.LAKEFS)]:
            metrics.record(func, tech=tech, filepath=merged_filepath)
            metrics.export()


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
