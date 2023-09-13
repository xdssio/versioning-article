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
                    'lfs-s2': helper.lfs_s3_upload,
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


def _append(tech: str, step: int, start_rows: int, add_rows: int, suffix: str, numeric: bool, label: str, seed: int,
            tracker: Tracker):
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
    tracker.latest
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
        numeric: Annotated[bool, typer.Option(help="Whether to generate numeric data")] = False,
        label: Annotated[str, typer.Option(help="The experiment to run", )] = 'default',
        seed: Annotated[int, typer.Option(help="The seed to use")] = 0):
    tracker = get_default_tracker()
    _append(tech, step, start_rows, add_rows, suffix, numeric, label, seed, tracker)


@app.command()
def compare(workflow: Annotated[Workflows, typer.Argument(help="The workflow to execute")],
            tech: Annotated[List[Tech], typer.Argument(help="The tech to use")],
            steps: Annotated[int, typer.Option(help="number of steps to run", min=1)] = 1,
            start_rows: Annotated[int, typer.Option(help="How many rows to start with")] = 100000000,
            add_rows: Annotated[int, typer.Option(help="How many rows to add")] = 10000000,
            suffix: Annotated[Suffix, typer.Option(help="What file type to save", )] = Suffix.csv,
            numeric: Annotated[bool, typer.Option(help="Whether to generate numeric data")] = False,
            label: Annotated[str, typer.Option(help="The experiment to run", )] = 'default',
            seed: Annotated[int, typer.Option(help="The seed to use")] = 0):
    tracker = get_default_tracker()
    if workflow == Workflows.append:
        workflow = _append
        kwargs = {'suffix': suffix,
                  'start_rows': start_rows,
                  'add_rows': add_rows,
                  'numeric': numeric,
                  'label': label, 'seed': seed, 'tracker': tracker}
    elif workflow == Workflows.split:
        workflow = split
    elif workflow == Workflows.taxi:
        workflow = taxi
    else:
        raise ValueError(f"Unknown workflow {workflow.name}")

    for step in tqdm(range(steps)):
        for t in tech:
            kwargs.update({'tech': t.name, 'step': step})
            workflow(**kwargs)


@app.command()
def test():
    for tech in upload_functions:
        logger.info(f"test {tech}")
        append(tech=tech, step=0, start_rows=10, add_rows=10, suffix='csv', numeric=True, label='test')


@app.command()
def latest(rows: Annotated[int, typer.Argument(help="Number of rows")] = 7):
    result = Reader(OUTPUT_DB).to_df()
    result = result[['timestamp','workflow', 'tech', 'step', 'file_size', 'name', 'time']]
    typer.echo(result.tail(rows).to_markdown())


if __name__ == "__main__":
    app()
