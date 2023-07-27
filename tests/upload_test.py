import time

from src.utils.git import GitHelper
from src.utils import generate_mock_data
import pytest

from tests.utils import s3_file_count

filename = '0.parquet'
filepath = f"tests/mock/{filename}"
helper = GitHelper()


@pytest.fixture(scope="session", autouse=True)
def before_tests():
    generate_mock_data('tests/mock', file_count=1, num_rows=100)


def test_dvc_upload():
    start_count = s3_file_count('dvc')
    helper.dvc_upload(filepath)
    assert s3_file_count('dvc') == start_count + 1


def test_lfs_upload():
    start_count = s3_file_count('git-lfs')
    helper.lfs_upload(filepath)
    assert s3_file_count('git-lfs') == start_count + 1


def test_xethub_upload():
    if filename in helper.xet_ls():
        helper.remove(filename)
        time.sleep(5)

    helper.xethub_upload(filepath)
    time.sleep(10)
    assert filename in helper.xet_ls()
