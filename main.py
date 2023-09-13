import os
import shutil
import typer
from typing import List
from tqdm import tqdm
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


def get_default_tracker():
    return Tracker(OUTPUT_DB, verbose=False, params={'pyxet': pyxet.__version__,
                                                     'gitxet': gitxet_version,
                                                     'branch': branch},
                   logger=logger)


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


class Suffix(str, Enum):
    csv = "csv"
    parquet = "parquet"
    txt = "txt"


@app.command()
def split():
    _split()


@app.command()
def taxi():
    _taxi()


def _taxi():
    raise NotImplementedError


def _split():
    raise NotImplementedError


def _append(tech: str,
            step: int,
            start_rows: int,
            add_rows: int,
            suffix: str,
            diverse: bool,
            label: str,
            seed: int,
            tracker: Tracker):
    numeric = not diverse
    params = {'workflow': 'append',
              'label': label,
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
    logger.info(f"running {func.__name__}")
    tracker.track(func, params=params, args=[filepath])
    # cleanup
    if os.path.exists(filename): os.remove(filename)
    if os.path.exists(f"{COPY_REPOS.get(tech)}/{filename}"): os.remove(f"{COPY_REPOS.get(tech)}/{filename}")


@app.command()
def append(
        tech: Annotated[Tech, typer.Option(help="The tech to use")] = Tech.pyxet,
        step: Annotated[int, typer.Option(help="The step to simulate", min=0)] = 0,
        start_rows: Annotated[int, typer.Option(help="How many rows to start with")] = 100000000,
        add_rows: Annotated[int, typer.Option(help="How many rows to add")] = 10000000,
        suffix: Annotated[Suffix, typer.Option(help="What file type to save", )] = Suffix.csv,
        diverse: Annotated[bool, typer.Option(help="Whether to generate numeric data")] = False,
        label: Annotated[str, typer.Option(help="The experiment to run", )] = 'default',
        seed: Annotated[int, typer.Option(help="The seed to use")] = 0):
    tracker = get_default_tracker()
    _append(tech, step, start_rows, add_rows, suffix, diverse, label, seed, tracker)


@app.command()
def benchmark(workflow: Annotated[Workflows, typer.Argument(help="The workflow to execute")],
              tech: Annotated[List[Tech], typer.Argument(help="The tech to use")] = None,
              steps: Annotated[int, typer.Option(help="number of steps to run", min=1)] = 1,
              start_rows: Annotated[int, typer.Option(help="How many rows to start with")] = 100000000,
              add_rows: Annotated[int, typer.Option(help="How many rows to add")] = 10000000,
              suffix: Annotated[Suffix, typer.Option(help="What file type to save", )] = Suffix.csv,
              diverse: Annotated[
                  bool, typer.Option(help="If True generate diverse data, default is numeric only")] = False,
              label: Annotated[str, typer.Option(help="The experiment to run", )] = None,
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
    tracker = get_default_tracker()
    if workflow == Workflows.append:
        if label is None:
            if steps == 1:
                label = "random"
            else:
                label = f"append-{steps}"
        run = _append
        kwargs = {'suffix': suffix,
                  'start_rows': start_rows,
                  'add_rows': add_rows,
                  'diverse': diverse,
                  'label': label,
                  'seed': seed,
                  'tracker': tracker}
    elif workflow == Workflows.split:
        run = split
    elif workflow == Workflows.taxi:
        run = taxi
    else:
        raise ValueError(f"Unknown workflow {workflow.name}")

    if not tech:
        tech = upload_functions.keys()
    logger.info(f"Running {workflow} with {tech} for {steps} steps")
    for step in tqdm(range(steps)):
        for t in tech:
            kwargs.update({'tech': t, 'step': step})
            run(**kwargs)


@app.command()
def test(seed: Annotated[int, typer.Option(help="The seed to use")] = 0):
    """Run a small test to make sure everything works"""
    tracker = get_default_tracker()
    for tech in tqdm(upload_functions):
        logger.info(f"test {tech}")
        _append(tech=tech,
                step=0,
                start_rows=10,
                add_rows=10,
                suffix='csv',
                diverse=False,
                label='test',
                seed=seed,
                tracker=tracker)


@app.command()
def latest(rows: Annotated[int, typer.Argument(
    help="Number of rows, if not provided, print all that belong to the latest experiment")] = None):
    reader = Reader(OUTPUT_DB)
    result = reader.to_df()
    if rows is not None:
        result = result.tail(rows)
    else:
        latest_track = result.tail(1)['track_id'].iloc[0]
        result = result[result['track_id'] == latest_track]
    result = result[
        ['name', 'time', 'label', 'tech', 'step', 'seed', 'workflow', 'file_size', 'track_id', 'timestamp', 'branch']]
    typer.echo(result.to_markdown())


if __name__ == "__main__":
    app()
