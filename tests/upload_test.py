import contextlib
import time
import git
from src.utils.helper import Helper
from src.utils.generators import DataFrameGenerator, NumericDataGenerator
import pytest
from tempfile import TemporaryDirectory

filename = '0.parquet'
filepath = f"tests/mock/{filename}"
helper = Helper()


@pytest.fixture(scope="session", autouse=True)
def before_tests():
    DataFrameGenerator().generate_mock_files('tests/mock', file_count=1, num_rows=300)


def test_dvc_upload():
    start_count = helper.s3_file_count('dvc')
    helper.copy_file(filepath, helper.DVC)
    helper._dvc_upload(filepath)
    assert helper.s3_file_count('dvc') == start_count + 1


def test_lfs_s3_upload():
    start_count = helper.s3_file_count('lfs')
    helper.copy_file(filepath, helper.LFS_S3)
    helper._lfs_upload(filepath, helper.LFS_S3)
    assert helper.s3_file_count('lfs') == start_count + 1


def test_lfs_git_upload():
    file_exists = helper.git_exists(filename, helper.LFS_GITHUB)
    if file_exists:
        helper.git_remove(filename, helper.LFS_GITHUB)
    helper.copy_file(filepath, helper.LFS_GITHUB)
    helper._lfs_upload(filepath, helper.LFS_GITHUB)
    assert helper.git_exists(filename, helper.LFS_GITHUB)


def test_xethub_py_upload():
    if filename in helper.xet_ls(helper.xet_pyxet_repo):
        helper.xet_remove(filename, helper.xet_pyxet_repo)
        time.sleep(5)

    helper._xethub_upload(filepath, pyxet_api=True)
    time.sleep(10)
    assert filename in helper.xet_ls(helper.xet_pyxet_repo)


def test_xethub_git_upload():
    if filename in helper.xet_ls(helper.xet_git_repo):
        helper.xet_remove(filename, helper.xet_git_repo)
        time.sleep(5)

    helper.copy_file(filepath, helper.XETHUB_GIT)
    helper._xethub_upload(filepath, pyxet_api=False)
    time.sleep(10)
    assert filename in helper.xet_ls(helper.xet_git_repo)
    with contextlib.suppress(RuntimeError):
        helper.xet_remove(filename, helper.xet_git_repo)


def test_s3_upload():
    if helper.s3_file_exists(filepath):
        helper.s3_remove(filepath)
    helper._s3_upload(filepath)
    assert helper.s3_file_exists(filepath)


def test_lakefs_upload():
    start_count = helper.s3_file_count(helper.LAKEFS)
    helper._lakefs_upload(filepath)
    assert helper.s3_file_count(helper.LAKEFS) == start_count + 1


def test_copy_pyxet():
    tmp = TemporaryDirectory()
    repo = git.Repo('xethub-py')
    xetpath = repo.remotes.origin.url.replace('https://', 'xet://') + '/main/mock/numeric.csv'
    xetpath = '/'.join(['xet:/'] + repo.remotes.origin.url.split('/')[-2:] + ['main', 'mock', 'numeric.csv'])
    xetpath = xetpath.replace('.git', '')
    # xetpath = "xet://xdssio/xethub-py-2/main/mock/numeric.csv"
    generator = NumericDataGenerator(cols=10)

    generator.export(generator.generate_data(100), tmp.name + '/numeric.csv')

    results = helper.xet_copy_time(tmp.name + '/numeric.csv', xetpath)
