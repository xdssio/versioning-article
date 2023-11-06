import contextlib
import os
import shutil
import time

from alive_progress import alive_bar
import typer
from typing_extensions import Annotated
from enum import Enum
import pyxet
from typing import List
from src.generators import DataFrameGenerator
from src.helper import Helper
import subprocess
from src.logger import Logger

LOGS = 'logs'
helper = Helper()
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

#
gitxet_version = subprocess.run(
    "git xet --version", shell=True, capture_output=True).stdout.decode('utf-8')
branch = subprocess.run("git branch --show-current", shell=True, capture_output=True).stdout.decode('utf-8').replace(
    '\n', '')
app = typer.Typer()
helper = Helper()
logger = Logger('logs', {'pyxet': pyxet.__version__,
                         'gitxet': gitxet_version,
                         'branch': branch})


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
    split = "split"  # type: ignore
    taxi = "taxi"
    features = "features"


class Suffix(str, Enum):
    csv = "csv"
    parquet = "parquet"
    txt = "txt"


def track(func: callable, args: list):
    logger.info(f"running {func.__name__}")
    params = {'function_name': func.__name__}
    start_time = time.time()
    func(*args)
    params['time'] = time.time()-start_time
    logger.log(params)


@app.command()
def pull():
    """Pull all repos to make sure we have the latest files in all repositories"""
    _pull()


def _pull():
    command = """
        cd lfs-github && git pull && cd .. && \
        cd lfs-s3 && git pull && cd .. && \
        cd dvc && git pull && cd .. && \
        cd xethub-git && git pull && cd .. 
        """
    helper.run(command, verbose=False)


def _taxi():
    raise NotImplementedError


def _features(tech: str,
              step: int,
              start_rows: int,
              suffix: Suffix,
              seed: int,
              label: str = 'default'):
    params = {'workflow': 'feature-engineering',
              'tech': tech,
              'step': step,
              'suffix': suffix,
              'seed': seed,
              'start_rows': start_rows,
              'numeric': True,
              'merge': True,
              'label': label}
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
    logger.params.update(params)
    if tech in COPY_REPOS:
        logger.info(f"Copying file to {COPY_REPOS[tech]}")
        shutil.copyfile(filepath, f"{COPY_REPOS[tech]}/{filename}")
    func = upload_functions.get(tech)
    track(func, [filepath])

    # cleanup
    if os.path.exists(filepath):
        os.remove(filepath)
    if os.path.exists(f"{COPY_REPOS.get(tech)}/{filename}"):
        os.remove(f"{COPY_REPOS.get(tech)}/{filename}")


def _split(tech: str,
           step: int,
           start_rows: int,
           add_rows: int,
           suffix: Suffix,
           seed: int,  # type: ignore
           label: str = 'default'):
    params = {'workflow': 'split',
              'tech': tech,
              'step': step,
              'suffix': suffix,
              'seed': seed,
              'start_rows': start_rows,
              'add_rows': add_rows,
              'numeric': True,
              'merge': True,
              'label': label}     # type: ignore
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
            shutil.copyfile(
                path, f"{COPY_REPOS[tech]}/{os.path.basename(path)}")
    func: callable = upload_functions.get(tech)
    with contextlib.suppress(KeyError):
        start_time = time.time()
        for filepath in (train_path, test_path, validation_path):
            if hasattr(func, '__name__'):
                logger.info(f"running {func.__name__} on {filepath}")
            params['file_size'] = helper.get_file_size(filepath)
            params['filename'] = os.path.basename(filepath)
            logger.params.update(params)
            """
            This create another line per file of train, test and validation - but it makes the output file less consistent  
            """
            # track(func, [filepath])
        params['time'] = time.time() - start_time
        params['file_size'] = params['file_size'] + (params['file_size'] * 2)
        params['filename'] = f"splits.{suffix}"
        params['function'] = f"split-{func.__name__}"
        logger.log(params)
    # cleanup
    for filepath in (train_path, test_path, validation_path):
        if os.path.exists(filepath):
            os.remove(filepath)
        if os.path.exists(f"{COPY_REPOS.get(tech)}/{os.path.basename(filepath)}"):
            os.remove(
                f"{COPY_REPOS.get(tech)}/{os.path.basename(filepath)}")


def _append(tech: str,
            step: int,
            start_rows: int,
            add_rows: int,
            suffix: str,
            diverse: bool,
            seed: int,
            label: str = 'default'):
    numeric = not diverse
    params = {'workflow': 'append',
              'tech': tech,
              'step': step,
              'suffix': suffix,
              'seed': seed,
              'start_rows': start_rows,
              'add_rows': add_rows,
              'numeric': numeric,
              'merge': True,
              'label': label}

    generator = DataFrameGenerator(seed=seed, numeric=numeric)
    filename = f"append.{suffix}"
    filepath = f"data/{filename}"
    df = generator.generate(start_rows + (add_rows * step))
    generator.export(df, filepath)
    params['file_size'] = helper.get_file_size(filepath)
    params['filename'] = filename
    logger.params.update(params)
    if tech in COPY_REPOS:
        logger.info(f"Copying file to {COPY_REPOS[tech]}")
        shutil.copyfile(filepath, f"{COPY_REPOS[tech]}/{filename}")
    func = upload_functions.get(tech)
    with contextlib.suppress(KeyError):
        logger.info(f"running {func.__name__}")
        track(func, [filepath])
    # cleanup
    if os.path.exists(filepath):
        os.remove(filepath)
    if os.path.exists(f"{COPY_REPOS.get(tech)}/{filename}"):
        os.remove(f"{COPY_REPOS.get(tech)}/{filename}")


@app.command()
def split(tech: Annotated[Tech, typer.Option(help="The tech to use")] = Tech.pyxet,
          step: Annotated[int, typer.Option(
              help="The step to simulate", min=0)] = 0,
          start_rows: Annotated[int, typer.Option(
              help="How many rows to start with")] = 100000000,
          add_rows: Annotated[int, typer.Option(
              help="How many rows to add")] = 10000000,
          suffix: Annotated[Suffix, typer.Option(
              help="What file type to save", )] = Suffix.parquet,
          label: Annotated[str, typer.Option(
              help="The experiment to run")] = 'default',
          seed: Annotated[int, typer.Option(help="The seed to use")] = 0):
    """run a single split experiment on a specific tech in a specific step"""
    _split(tech, step, start_rows, add_rows, suffix, seed, label)


@app.command()
def taxi():
    """Run a benchamrks using the taxi dataset - currently not implemented"""
    _taxi()


@app.command()
def features(tech: Annotated[Tech, typer.Option(help="The tech to use")] = Tech.pyxet,
             step: Annotated[int, typer.Option(
                 help="The step to simulate", min=0)] = 0,
             start_rows: Annotated[int, typer.Option(
                 help="How many rows to start with")] = 100000000,
             suffix: Annotated[Suffix, typer.Option(
                 help="What file type to save", )] = Suffix.parquet,
             label: Annotated[str, typer.Option(
                 help="The experiment to run")] = 'default',
             seed: Annotated[int, typer.Option(help="The seed to use")] = 0):
    """run a single features engineering experiment on a specific tech in a specific step"""
    _features(tech, step, start_rows, suffix, seed, label)


@app.command()
def append(
        tech: Annotated[Tech, typer.Option(
            help="The tech to use")] = Tech.pyxet,
        step: Annotated[int, typer.Option(
            help="The step to simulate", min=0)] = 0,
        start_rows: Annotated[int, typer.Option(
            help="How many rows to start with")] = 100000000,
        add_rows: Annotated[int, typer.Option(
            help="How many rows to add")] = 10000000,
        suffix: Annotated[Suffix, typer.Option(
            help="What file type to save", )] = Suffix.parquet,
        diverse: Annotated[bool, typer.Option(
            help="Whether to generate numeric data")] = False,
        label: Annotated[str, typer.Option(
            help="The experiment to run", )] = 'default',
        seed: Annotated[int, typer.Option(help="The seed to use")] = 0):
    """run a single append experiment on a specific tech in a specific step"""
    _append(tech, step, start_rows, add_rows,
            suffix, diverse, seed, label=label)


@app.command()
def benchmark(workflow: Annotated[Workflows, typer.Argument(help="The workflow to execute")],
              tech: Annotated[List[Tech], typer.Option(
                  help="The tech to use")] = None,
              steps: Annotated[int, typer.Option(
                  help="number of steps to run", min=1)] = 1,
              start_rows: Annotated[int, typer.Option(
                  help="How many rows to start with")] = 100000000,
              add_rows: Annotated[int, typer.Option(
                  help="How many rows to add")] = 10000000,
              suffix: Annotated[Suffix, typer.Option(
                  help="What file type to save", )] = Suffix.parquet,
              diverse: Annotated[
                  bool, typer.Option(help="If True generate diverse data, default is numeric only")] = False,
              label: Annotated[str, typer.Option(
                  help="The experiment to run", )] = 'default',
              seed: Annotated[int, typer.Option(help="The seed to use")] = 0):
    """
    Benchmark different technologies - run a workflow with different technologies for a number of steps\n\n

    Examples:
    python main.py benchmark append --steps 1 --start-rows 10 --add-rows 10\n
    python main.py benchmark append s3 gitxet --steps 10 --start-rows 100000000 --add-rows 10000000 --suffix csv --label default --seed 0
    """
    _pull()
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
                  'seed': seed,
                  'label': label}
    elif workflow == Workflows.split:
        run = _split
        kwargs = {'suffix': suffix,
                  'start_rows': start_rows,
                  'add_rows': add_rows,
                  'seed': seed,
                  'label': label}
    elif workflow == Workflows.features:
        run = _features
        kwargs = {'suffix': suffix,
                  'start_rows': start_rows,
                  'seed': seed,
                  'label': label}

    elif workflow == Workflows.taxi:
        raise NotImplementedError("Taxi workflow is not implemented yet")
        run = taxi
    else:
        raise ValueError(f"Unknown workflow {workflow.name}")

    if not tech:
        tech = list(upload_functions.keys())
    logger.info(
        f"Running {workflow} with {str(tech).replace('Tech.', '')} for {steps} steps")
    with alive_bar(steps * len(tech)) as bar:
        for step in range(steps):
            for t in tech:
                kwargs.update({'tech': t, 'step': step})
                run(**kwargs)
                bar()


@app.command()
def test(seed: Annotated[int, typer.Option(help="The seed to use")] = 0):
    """Run a small test to make sure everything works"""
    _pull()
    with alive_bar(len(upload_functions)) as bar:
        for tech in upload_functions:
            logger.info(f"test {tech}")
            _append(tech=tech,
                    step=0,
                    start_rows=10,
                    add_rows=10,
                    suffix=Suffix.parquet,
                    diverse=False,
                    seed=seed)
            bar()


@app.command()
def latest(rows: Annotated[int, typer.Option(
        help="Number of rows, if not provided")] = 20,
        export: Annotated[bool, typer.Option(help="Whether to export to csv")] = False):
    """print to screen the latest n rows of the experiment"""

    result = logger.to_df(latest=True)
    result = result.tail(rows)
    if export:
        result.to_csv('output/latest.csv', index=False)
    columns = ['workflow', 'tech', 'time', 'step', 'seed', 'file_size', 'timestamp', 'branch',
               'filename', 'label', 'run_name']
    columns = [column for column in columns if column in result.columns]
    typer.echo(result[columns].to_markdown())


if __name__ == "__main__":
    app()
