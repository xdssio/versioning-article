import contextlib
import os
import shutil
import time

from alive_progress import alive_bar
import typer
from typing import List
from typing_extensions import Annotated
from enum import Enum
import pyxet
from xetrack import Tracker, Reader
from loguru import logger
from src.generators import DataFrameGenerator
from src.helper import Helper
import subprocess

helper = Helper()
OUTPUT_DB = 'output/stats.db'
COPY_REPOS = {'gitxet': Helper.XETHUB_GIT,
              'lfs-git': Helper.LFS_GITHUB,
              'lfs-s3': Helper.LFS_S3,
              'dvc': Helper.DVC}

upload_functions = {'pyxet': helper.pyxet_upload,
                    's3': helper.s3_upload,
                    'gitxet': helper.gitxet_upload,
                    'lakefs': helper.lakefs_upload,
                    'lfs-git': helper.lfs_git_upload,
                    'lfs-s3': helper.lfs_s3_upload,
                    'dvc': helper.dvc_upload,
                    }

logger.add("logs/{time:YYYY-MM-DD}.log", rotation="1 day")
gitxet_version = subprocess.run("git xet --version", shell=True, capture_output=True).stdout.decode('utf-8')
branch = subprocess.run("git branch --show-current", shell=True, capture_output=True).stdout.decode('utf-8').replace(
    '\n', '')
app = typer.Typer()
helper = Helper()


def get_default_tracker(label: str = None):
    params = {'pyxet': pyxet.__version__,
              'gitxet': gitxet_version,
              'branch': branch}
    if label:
        params['label'] = label
    return Tracker(OUTPUT_DB, params=params, logger=logger)


class Tech(str, Enum):
    s3 = "s3"
    pyxet = "pyxet"
    gitxet = "gitxet"
    lakefs = "lakefs"
    lfs_git = "lfs-git"
    lfs_s3 = "lfs-s3"
    dvc = "dvc"


class Workflows(str, Enum):
    append = "append"
    split = "split"
    taxi = "taxi"
    features = "features"


class Suffix(str, Enum):
    csv = "csv"
    parquet = "parquet"
    txt = "txt"


@app.command()
def split(tech: Annotated[Tech, typer.Option(help="The tech to use")] = Tech.pyxet,
          step: Annotated[int, typer.Option(help="The step to simulate", min=0)] = 0,
          start_rows: Annotated[int, typer.Option(help="How many rows to start with")] = 100000000,
          add_rows: Annotated[int, typer.Option(help="How many rows to add")] = 10000000,
          suffix: Annotated[Suffix, typer.Option(help="What file type to save", )] = Suffix.parquet,
          label: Annotated[str, typer.Option(help="The experiment to run")] = 'default',
          seed: Annotated[int, typer.Option(help="The seed to use")] = 0):
    tracker = get_default_tracker(label=label)
    _split(tech, step, start_rows, add_rows, suffix, seed, tracker)


@app.command()
def taxi():
    _taxi()


def _taxi():
    raise NotImplementedError


@app.command()
def features(tech: Annotated[Tech, typer.Option(help="The tech to use")] = Tech.pyxet,
             step: Annotated[int, typer.Option(help="The step to simulate", min=0)] = 0,
             start_rows: Annotated[int, typer.Option(help="How many rows to start with")] = 100000000,
             suffix: Annotated[Suffix, typer.Option(help="What file type to save", )] = Suffix.parquet,
             label: Annotated[str, typer.Option(help="The experiment to run")] = 'default',
             seed: Annotated[int, typer.Option(help="The seed to use")] = 0):
    tracker = get_default_tracker(label=label)
    _features(tech, step, start_rows, suffix, seed, tracker)


def _features(tech: str,
              step: int,
              start_rows: int,
              suffix: str,
              seed: int,
              tracker: Tracker):
    params = {'workflow': 'feature-engineering',
              'tech': tech,
              'step': step,
              'suffix': suffix,
              'seed': seed,
              'start_rows': start_rows,
              'numeric': True,
              'merge': True}
    if tracker is None:
        logger.debug("No tracker provided, creating default tracker")
        tracker = get_default_tracker(label='default')
    os.makedirs('data', exist_ok=True)
    generator = DataFrameGenerator(seed=seed, numeric=True)
    filename = f"features.{suffix}"
    filepath = f"data/{filename}"
    df = generator.generate(start_rows)
    data_features = generator.generate_features(start_rows, step)
    for column in data_features.columns:
        df[column] = data_features[column]

    generator.export(df, filepath)
    params['file_size'] = helper.get_file_size(filepath)
    params['filename'] = filename
    if tech in COPY_REPOS:
        logger.info(f"Copying file to {COPY_REPOS[tech]}")
        shutil.copyfile(filepath, f"{COPY_REPOS[tech]}/{filename}")
    func = upload_functions.get(tech)
    with contextlib.suppress(KeyError):
        logger.info(f"running {func.__name__}")
        tracker.track(func, params=params, args=[filepath])
    # cleanup
    if os.path.exists(filepath): os.remove(filepath)
    if os.path.exists(f"{COPY_REPOS.get(tech)}/{filename}"): os.remove(f"{COPY_REPOS.get(tech)}/{filename}")


def _split(tech: str,
           step: int,
           start_rows: int,
           add_rows: int,
           suffix: str,
           seed: int,
           tracker: Tracker):
    params = {'workflow': 'split',
              'tech': tech,
              'step': step,
              'suffix': suffix,
              'seed': seed,
              'start_rows': start_rows,
              'add_rows': add_rows,
              'numeric': True,
              'merge': True}
    if tracker is None:
        logger.debug("No tracker provided, creating default tracker")
        tracker = get_default_tracker(label='default')
    os.makedirs('data', exist_ok=True)
    generator = DataFrameGenerator(seed=seed, numeric=True)

    train_size = start_rows + (add_rows * step)
    df = generator.generate(train_size + (2 * add_rows))
    train, validation, test = df.iloc[:train_size], df.iloc[train_size:train_size + add_rows], df.iloc[
                                                                                               train_size + add_rows:]
    train_path, test_path, validation_path = f"data/train.{suffix}", f"data/test.{suffix}", f"data/validation.{suffix}"
    for df, path in [(train, train_path), (test, test_path), (validation, validation_path)]:
        generator.export(df, path)

    if tech in COPY_REPOS:
        logger.info(f"Copying file to {COPY_REPOS[tech]}")
        for path in [train_path, test_path, validation_path]:
            shutil.copyfile(path, f"{COPY_REPOS[tech]}/{os.path.basename(path)}")
    func = upload_functions.get(tech)
    with contextlib.suppress(KeyError):
        start_time = time.time()
        for filepath in [train_path, test_path, validation_path]:
            logger.info(f"running {func.__name__} on {filepath}")
            params['file_size'] = helper.get_file_size(filepath)
            params['filename'] = os.path.basename(filepath)
            tracker.track(func, params=params, args=[filepath])
        params['time'] = time.time() - start_time
        params['file_size'] = params['file_size'] + (params['file_size'] * 2)
        params['filename'] = f"splits.{suffix}"
        params['function'] = f"split-{func.__name__}"
        params['name'] = f"{tech}-split"
        params['tech'] = tech
        tracker.log(**params)
    # cleanup
    for filepath in [train_path, test_path, validation_path]:
        if os.path.exists(filepath): os.remove(filepath)
        if os.path.exists(f"{COPY_REPOS.get(tech)}/{os.path.basename(filepath)}"): os.remove(
            f"{COPY_REPOS.get(tech)}/{os.path.basename(filepath)}")


def _append(tech: str,
            step: int,
            start_rows: int,
            add_rows: int,
            suffix: str,
            diverse: bool,
            seed: int,
            tracker: Tracker):
    numeric = not diverse
    params = {'workflow': 'append',
              'tech': tech,
              'step': step,
              'suffix': suffix,
              'seed': seed,
              'start_rows': start_rows,
              'add_rows': add_rows,
              'numeric': numeric,
              'merge': True}
    if tracker is None:
        logger.debug("No tracker provided, creating default tracker")
        tracker = get_default_tracker()

    generator = DataFrameGenerator(seed=seed, numeric=numeric)
    filename = f"append.{suffix}"
    filepath = f"data/{filename}"
    df = generator.generate(start_rows + (add_rows * step))
    generator.export(df, filepath)
    params['file_size'] = helper.get_file_size(filepath)
    params['filename'] = filename
    if tech in COPY_REPOS:
        logger.info(f"Copying file to {COPY_REPOS[tech]}")
        shutil.copyfile(filepath, f"{COPY_REPOS[tech]}/{filename}")
    func = upload_functions.get(tech)
    with contextlib.suppress(KeyError):
        logger.info(f"running {func.__name__}")
        tracker.track(func, params=params, args=[filepath])
    # cleanup
    if os.path.exists(filepath): os.remove(filepath)
    if os.path.exists(f"{COPY_REPOS.get(tech)}/{filename}"): os.remove(f"{COPY_REPOS.get(tech)}/{filename}")


@app.command()
def append(
        tech: Annotated[Tech, typer.Option(help="The tech to use")] = Tech.pyxet,
        step: Annotated[int, typer.Option(help="The step to simulate", min=0)] = 0,
        start_rows: Annotated[int, typer.Option(help="How many rows to start with")] = 100000000,
        add_rows: Annotated[int, typer.Option(help="How many rows to add")] = 10000000,
        suffix: Annotated[Suffix, typer.Option(help="What file type to save", )] = Suffix.parquet,
        diverse: Annotated[bool, typer.Option(help="Whether to generate numeric data")] = False,
        label: Annotated[str, typer.Option(help="The experiment to run", )] = 'default',
        seed: Annotated[int, typer.Option(help="The seed to use")] = 0):
    tracker = get_default_tracker(label=label)
    _append(tech, step, start_rows, add_rows, suffix, diverse, seed, tracker)


@app.command()
def benchmark(workflow: Annotated[Workflows, typer.Argument(help="The workflow to execute")],
              tech: Annotated[List[Tech], typer.Option(help="The tech to use")] = None,
              steps: Annotated[int, typer.Option(help="number of steps to run", min=1)] = 1,
              start_rows: Annotated[int, typer.Option(help="How many rows to start with")] = 100000000,
              add_rows: Annotated[int, typer.Option(help="How many rows to add")] = 10000000,
              suffix: Annotated[Suffix, typer.Option(help="What file type to save", )] = Suffix.parquet,
              diverse: Annotated[
                  bool, typer.Option(help="If True generate diverse data, default is numeric only")] = False,
              label: Annotated[str, typer.Option(help="The experiment to run", )] = 'default',
              seed: Annotated[int, typer.Option(help="The seed to use")] = 0):
    """
    Benchmark different technologies

    :param workflow: Which workflow to run - can be append, split or taxi
    :param tech: Which tech to use - can be s3, pyxet, gitxet, lakefs, lfs-git, lfs-s3 or dvc, None means all
    :param steps: How many steps to run
    :param start_rows: How many rows to start with
    :param add_rows: How many rows to add in each step
    :param suffix: What file type to save - can be csv, parquet or txt
    :param diverse: If True generate diverse data, default is numeric only
    :param label: A label for the experiment
    :param seed: The seed to use for each step
    :return:

    Examples:
    python main.py benchmark append --steps 1 --start-rows 10 --add-rows 10
    python main.py benchmark append s3 gitxet --steps 10 --start-rows 100000000 --add-rows 10000000 --suffix csv --label default --seed 0
    """
    if workflow == Workflows.append:
        if label is None:
            if steps == 1:
                label = "random"
            else:
                label = f"append-{steps}"
        run = _append
        tracker = get_default_tracker(label=label)
        kwargs = {'suffix': suffix,
                  'start_rows': start_rows,
                  'add_rows': add_rows,
                  'diverse': diverse,
                  'seed': seed,
                  'tracker': tracker}
    elif workflow == Workflows.split:
        run = _split
        tracker = get_default_tracker(label=label)
        kwargs = {'suffix': suffix,
                  'start_rows': start_rows,
                  'add_rows': add_rows,
                  'seed': seed,
                  'tracker': tracker}
    elif workflow == Workflows.features:
        run = _features
        tracker = get_default_tracker(label=label)
        kwargs = {'suffix': suffix,
                  'start_rows': start_rows,
                  'seed': seed,
                  'tracker': tracker}

    elif workflow == Workflows.taxi:
        raise NotImplementedError("Taxi workflow is not implemented yet")
        run = taxi
    else:
        raise ValueError(f"Unknown workflow {workflow.name}")

    if not tech:
        tech = list(upload_functions.keys())
    logger.info(f"Running {workflow} with {str(tech).replace('Tech.', '')} for {steps} steps")
    with alive_bar(steps * len(tech)) as bar:
        for step in range(steps):
            for t in tech:
                kwargs.update({'tech': t, 'step': step})
                run(**kwargs)
                bar()


@app.command()
def test(seed: Annotated[int, typer.Option(help="The seed to use")] = 0):
    """Run a small test to make sure everything works"""
    tracker = get_default_tracker(label='test')
    with alive_bar(len(upload_functions)) as bar:
        for tech in upload_functions:
            logger.info(f"test {tech}")
            _append(tech=tech,
                    step=0,
                    start_rows=10,
                    add_rows=10,
                    suffix=Suffix.parquet,
                    diverse=False,
                    seed=seed,
                    tracker=tracker)
            bar()


@app.command()
def latest(rows: Annotated[int, typer.Argument(
    help="Number of rows, if not provided, print all that belong to the latest experiment")] = None,
           export: Annotated[bool, typer.Option(help="Whether to export to csv")] = False):
    reader = Reader(OUTPUT_DB)
    result = reader.to_df()
    if rows is not None:
        result = result.tail(rows)
    else:
        latest_track = result.tail(1)['track_id'].iloc[0]
        result = result[result['track_id'] == latest_track]
    if export:
        result.to_csv('output/latest.csv', index=False)
    columns = ['name', 'function_time', 'label', 'tech', 'step', 'seed', 'workflow', 'file_size', 'timestamp', 'branch',
               'filename', 'track_id']
    typer.echo(result[columns].to_markdown())


if __name__ == "__main__":
    app()
