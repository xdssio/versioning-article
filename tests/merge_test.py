import os
import shutil

from src.utils import merge, generate_mock_data
from glob import glob


def test_generate_mock_data():
    generate_mock_data('mock', 5, 100)
    assert len(glob('mock/*.parquet')) == 5
    shutil.rmtree('mock', ignore_errors=True)


def test_merge():
    generate_mock_data('mock', 5, 10000)
    files = glob('mock/*.parquet')
    for file in files:
        merge('merged.parquet', file)
    os.remove('merged.parquet')


if __name__ == '__main__':
    test_merge()
