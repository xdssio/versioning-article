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
OUTPUT_DB = 'output/stats.db'


def benchmark_random(iterations: int = 100):
    logger.info(f"benchmark - random - {iterations} files")
    stop = False  # for graceful exit
    filename = f"data.parquet"
    filepath = f"random/{filename}"
    tracker = Tracker(OUTPUT_DB, verbose=False,
                      params={'merged': True, 'step': -1, 'filename': filepath, 'tech': Helper.M1,
                              'workflow': 'random'})
    generator = DataFrameGenerator(num_rows=10000)  # ~ 1.2MB
    for step in tqdm(range(iterations)):
        try:
            if stop:
                break
            df = generator.generate_data()
            df.to_parquet(filepath)
            tracker.set_params({'step': step, 'file_bytes': helper.get_file_size(filepath)})

            for repo in [helper.DVC, helper.LFS_S3, Helper.LFS_GITHUB, Helper.XETHUB_GIT]:
                new_filepath = os.path.join(repo, filename)
                df.to_parquet(new_filepath, index=False)
            for func in (helper.gitxet_new_upload,
                         helper.dvc_new_upload,
                         helper.lfs_s3_new_upload,
                         helper.s3_new_upload,
                         helper.lfs_git_new_upload,
                         helper.pyxet_new_upload,
                         helper.lakefs_new_upload):
                try:
                    tracker.track_function(func, filepath=filepath)
                except KeyboardInterrupt:
                    stop = True
        except KeyboardInterrupt as e:
            logger.info(f"KeyboardInterrupt: {e}")
            break


def benchmark_append(iterations: int = 100):
    logger.info(f"benchmark - blog - {iterations} iterations")
    original = 'blog/original.csv'
    appended_filepath = filepath = 'blog/blog.csv'
    tracker = Tracker(OUTPUT_DB, verbose=False,
                      params={'merged': False,
                              'step': -1,
                              'filename': appended_filepath,
                              'tech': Helper.M1,
                              'workflow': 'append'})

    data = pd.read_csv(original, nrows=68200)
    generator = BlogRowsGenerator(counter=data['id'].max(),
                                  date=data['date'].max(),
                                  genders=list(set(data['gender'])),
                                  topics=list(set(data['topic'])),
                                  signs=list(set(data['sign'])),
                                  )
    shutil.copyfile(original, filepath)
    filename = path.basename(filepath)
    for repo in [helper.DVC, helper.LFS_S3, Helper.LFS_GITHUB, Helper.XETHUB_GIT]:
        tracker.track_function(helper.copy_file, filepath=filepath, repo=repo)

    stop = False  # for graceful exit
    for step in range(iterations):
        try:
            if stop:
                break
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
    file_count = len(files)
    merged_filepath = path.join('taxi', 'merged.parquet')
    if merged_filepath in files:
        file_count -= 1
    logger.info(f"benchmark - taxi : {file_count} files")
    tracker = Tracker(OUTPUT_DB, verbose=False, params={'step': -1,
                                                        'file_count': file_count,
                                                        'workflow': 'taxi'})

    stop = False  # for graceful exit
    for step, filepath in tqdm(enumerate(files)):
        if step == iterations or stop:
            break
        tracker.set_params(
            {'step': step,
             "filepath": filepath,
             'file_bytes': helper.get_file_size(filepath),
             'merged': False,
             'tech': Helper.M1, })

        tracker.track_function(helper.merge_files, new_filepath=filepath, merged_filepath=merged_filepath)

        # copy locally
        for repo in [helper.DVC, helper.LFS_S3, Helper.LFS_GITHUB, Helper.XETHUB_GIT]:
            tracker.track_function(helper.copy_file, filepath=filepath, repo=repo)
            tracker.track_function(helper.copy_file, filepath=merged_filepath, repo=repo)

        for func, tech in (helper.gitxet_new_upload,
                           helper.dvc_new_upload,
                           helper.lfs_s3_new_upload,
                           helper.s3_new_upload,
                           helper.lfs_git_new_upload,
                           helper.pyxet_new_upload,
                           helper.lakefs_new_upload):
            try:
                tracker.track_function(func, filepath=filepath)
            except KeyboardInterrupt:
                stop = True
        tracker.set_params({'merged': True,
                            'filepath': merged_filepath,
                            'file_bytes': helper.get_file_size(merged_filepath)})

        for func, tech in (helper.pyxet_merged_upload,
                           helper.dvc_merged_upload,
                           helper.lfs_s3_merged_upload,
                           helper.gitxet_merged_upload,
                           helper.lfs_git_merged_upload,
                           helper.s3_merged_upload,
                           helper.lakefs_merged_upload):
            try:
                tracker.track_function(func, filepath=filepath)
            except KeyboardInterrupt:
                stop = True


if __name__ == '__main__':
    p = argparse.ArgumentParser('Benchmarking of NYC Taxi data in different repositories')
    p.add_argument('workflow', choices=['append', 'taxi', 'random'], help='The experiment to run')
    p.add_argument('-i', '--iterations', type=int, help='Number of iterations to run', default=10)
    p.add_argument('-u',
                   '--upload', default=False, action='store_true',
                   help='If True, upload to repo')
    p.add_argument('-s',
                   '--show', default=False, action='store_true',
                   help='If True, run snakeviz server')
    args = p.parse_args()
    iterations = args.iterations
    if args.workflow == 'append':
        command = f"benchmark_append({iterations})"
    elif args.workflow == 'taxi':
        command = f"benchmark_taxi({iterations})"
    elif args.workflow == 'random':
        command = f"benchmark_random({iterations})"
    else:
        raise ValueError(f"Unknown experiment: {args.workflow}")

    profiler = cProfile.Profile()
    start = time.time()
    profiler.run(command)
    profiler.dump_stats('output/profile.prof')

    if args.upload:
        with contextlib.suppress(Exception):  # Not to break the flow
            helper.output_upload(f"Workflow {args.workflow} - {iterations} iterations")
    print(f"######### Total time: {time.time() - start} #########")
    if args.show:
        with contextlib.suppress(Exception):  # Not to break the flow
            print(helper.run("snakeviz output/profile.prof"))
