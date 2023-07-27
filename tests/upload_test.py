from src.utils.git import GitHelper
from src.utils import generate_mock_data
import pathlib
import shutil

from tests.utils import s3_file_count


def test_dvc_upload():
    basedir = 'dvcmock'
    file_path = f"{basedir}/0.parquet"
    generate_mock_data(basedir, file_count=1, num_rows=100)

    helper = GitHelper()
    start_count = s3_file_count('dvc')
    helper.dvc_upload(file_path, repo='dvc')
    assert s3_file_count('dvc') == start_count + 1
    shutil.rmtree(basedir)

def test_lfs_upload():
    basedir = 'lfsmock'
    file_path = f"{basedir}/0.parquet"
    generate_mock_data(basedir, file_count=1, num_rows=100)

    helper = GitHelper()
    start_count = s3_file_count('lfs')
    helper.lfs_upload(file_path, repo='lfs')
    assert s3_file_count('lfs') == start_count + 1
    shutil.rmtree(basedir)
