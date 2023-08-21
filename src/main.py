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
from src.utils.generators import BlogDataGenerator, DataFrameGenerator, NumericDataGenerator
from xetrack import Tracker
import pyxet
import subprocess

gitxet_version = subprocess.run("git xet --version", shell=True, capture_output=True).stdout.decode('utf-8')

logger.add("logs/{time}.log")
helper = Helper()
metrics = MetricsHelper()
OUTPUT_DB = 'output/stats.db'
copy_tech_repos = [Helper.XETHUB_GIT, Helper.LFS_GITHUB, Helper.LFS_S3, Helper.DVC]

upload_functions = (helper.gitxet_upload,
                    helper.lakefs_upload,
                    helper.dvc_upload,
                    helper.lfs_s3_upload,
                    helper.s3_upload,
                    helper.pyxet_upload,
                    helper.lfs_git_upload,
                    )


def copy_to_repos(filepath):
    for repo in copy_tech_repos:
        helper.copy_file(filepath, repo)


def run_functions(tracker, filepath):
    for func in upload_functions:
        logger.info(f"Running {func.__name__}")
        tracker.track(func, args=[filepath])


def benchmark_random(iterations: int = 100):
    logger.info(f"benchmark - random - {iterations} files")
    filename = f"data.parquet"
    filepath = f"random/{filename}"
    tracker = Tracker(OUTPUT_DB, verbose=False,
                      params={'merged': False,
                              'filename': filepath,
                              'workflow': 'random',
                              'pyxet': pyxet.__version__,
                              'gitxet': gitxet_version
                              })
    generator = DataFrameGenerator(num_rows=10000)  # ~ 1.2MB
    for step in tqdm(range(iterations)):
        df = generator.generate_data()
        df.to_parquet(filepath)
        tracker.set_params({'step': step, 'file_bytes': helper.get_file_size(filepath)})
        copy_to_repos(filepath)
        run_functions(tracker, filepath)

    logger.info(f"Cleanup...")
    for repo in copy_tech_repos:
        os.remove(f"{repo}/{filename}")


def benchmark_append(iterations: int = 100):
    logger.info(f"benchmark append csv - blog - {iterations} iterations")
    data_dir = 'blog'
    original = f"{data_dir}/original.csv"
    filename = 'appended.csv'
    n_rows_add = 1
    start_rows = 68200
    appended_filepath = filepath = f"{data_dir}/{filename}"
    tracker = Tracker(OUTPUT_DB, verbose=False,
                      params={'merged': True,
                              'filename': appended_filepath,
                              'workflow': 'append',
                              'pyxet': pyxet.__version__,
                              'gitxet': gitxet_version,
                              'file_bytes': helper.get_file_size(original),
                              'n_rows_add': n_rows_add,
                              'start_rows': start_rows,
                              })

    data = pd.read_csv(original, nrows=start_rows)
    generator = BlogDataGenerator(counter=data['id'].max(),
                                  date=data['date'].max(),
                                  genders=list(set(data['gender'])),
                                  topics=list(set(data['topic'])),
                                  signs=list(set(data['sign'])),
                                  )
    shutil.copyfile(original, filepath)  # copy original file
    copy_to_repos(filepath)  # copy to repos

    for step in range(iterations):
        tracker.set_params({'step': step, 'file_bytes': helper.get_file_size(filepath)})
        if step > 0:
            for repo in [data_dir] + copy_tech_repos:
                generator.append(os.path.join(repo, filename), n_rows_add)
        run_functions(tracker, filepath)  # run functions on a loop

    logger.info(f"Cleanup...")
    for repo in [data_dir] + copy_tech_repos:
        os.remove(os.path.join(repo, filename))


def benchmark_taxi_merged():
    data_dir = 'taxi'
    files = sorted(glob(f"{data_dir}/*.parquet"))
    file_count = len(files)
    merged_filename = 'merged.parquet'
    merged_filepath = path.join('taxi', merged_filename)
    if merged_filepath in files:
        file_count -= 1
    logger.info(f"benchmark - taxi : {file_count} files")
    tracker = Tracker(OUTPUT_DB,
                      verbose=False,
                      params={'step': -1,
                              'file_count': file_count,
                              'workflow': 'taxi-merged',
                              'pyxet': pyxet.__version__,
                              'gitxet': gitxet_version,
                              'merged': True,
                              })

    for step, filepath in tqdm(enumerate(files)):
        tracker.set_params({'step': step, "filepath": filepath, 'file_bytes': helper.get_file_size(filepath)})
        tracker.track(helper.merge_files, args=[filepath, merged_filepath])
        copy_to_repos(merged_filepath)
        run_functions(tracker, merged_filepath)

    logger.info(f"Cleanup...")
    for repo in ['taxi'] + copy_tech_repos:
        os.remove(os.path.join(repo, merged_filename))


def benchmark_taxi_new():
    data_dir = 'taxi'
    files = sorted(glob(f"{data_dir}/*.parquet"))
    file_count = len(files)
    logger.info(f"benchmark - taxi upload: {file_count} files")
    tracker = Tracker(OUTPUT_DB,
                      verbose=False,
                      params={'step': -1,
                              'file_count': file_count,
                              'workflow': 'taxi upload',
                              'pyxet': pyxet.__version__,
                              'gitxet': gitxet_version,
                              'merged': False,
                              })

    for step, filepath in tqdm(enumerate(files)):
        tracker.set_params({'step': step, 'file_bytes': helper.get_file_size(filepath), 'filepath': filepath})
        # copy locally
        copy_to_repos(filepath)
        run_functions(tracker, filepath)

    logger.info(f"Cleanup...")
    for repo in copy_tech_repos:
        files = os.listdir(repo)
        for file in files:
            if file.endswith('.parquet'):
                os.remove(os.path.join(repo, file))


def benchmark_numeric(iterations: int = 20):
    logger.info(f"benchmark - numeric - {iterations} iterations")
    n_rows_add = 100000
    start_rows = 1000000
    columns = 10
    data_dir = 'numeric'
    filename = 'numeric.csv'
    filepath = f"{data_dir}/{filename}"

    generator = NumericDataGenerator(cols=columns)
    df = generator.generate_data(start_rows)
    generator.to_csv(df, filepath)

    tracker = Tracker(OUTPUT_DB, verbose=False, log_system_params=True,
                      params={'workflow': 'numeric',
                              'n_rows_add': n_rows_add,
                              'start_rows': start_rows,
                              'columns': columns,
                              'filepath': filepath,
                              'pyxet': pyxet.__version__,
                              'gitxet': gitxet_version,
                              'merged': True,
                              'file_bytes': helper.get_file_size(filepath),
                              })

    copy_to_repos(filepath)  # copy to repos

    for step in tqdm(range(iterations)):
        tracker.set_params({'step': step, 'file_bytes': helper.get_file_size(filepath)})
        run_functions(tracker, filepath)  # run functions on a loop
        logger.debug("Appending rows")
        for repo in [data_dir] + copy_tech_repos:
            generator.append(os.path.join(repo, filename), n_rows_add)
    # Cleanup
    logger.info(f"Cleanup...")
    for repo in [data_dir] + copy_tech_repos:
        os.remove(os.path.join(repo, filename))

        """
        # Hoyt version
        xet_path = 'xet://xdssio/xethub-py/main/numeric.csv'
        s3_path = 's3:///versioning-article/s3/numeric.csv'
        for fuc, args in [(helper.s3_copy_time, [filepath, s3_path]),
                          (helper.xet_copy_time, [filepath, xet_path]),
                          (helper.lakefs_copy_time, [filepath])]:
            try:
                logger.info(f"Running {fuc.__name__}")
                tracker.track(fuc, args=args)
                logger.debug(tracker.last)
            except KeyboardInterrupt:
                stop = True
            generator.append(filepath, n_rows_add)
        os.remove(filepath)
        """


if __name__ == '__main__':
    p = argparse.ArgumentParser('Benchmarking of NYC Taxi data in different repositories')
    p.add_argument('workflow', choices=['append', 'taxi', 'random', 'numeric'], help='The experiment to run')
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
    elif args.workflow == 'numeric':
        command = f"benchmark_numeric({iterations})"
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
