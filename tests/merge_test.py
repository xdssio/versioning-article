from tempfile import TemporaryDirectory
from src.generators import DataFrameGenerator
from src.helper import Helper

from glob import glob


def test_generate_mock_data():
    tmp = TemporaryDirectory()
    path = tmp.name + '/mock'
    DataFrameGenerator().generate_mock_files(path, 5, 100)
    assert len(glob(path+'/*')) == 5


def test_merge():
    tmp = TemporaryDirectory()
    path = tmp.name + '/mock'
    DataFrameGenerator().generate_mock_files(path, 5, 100)
    files = glob(path+'/*')
    helper = Helper()
    for file in files:
        helper.merge_files(file, path+'/merged.parquet')


