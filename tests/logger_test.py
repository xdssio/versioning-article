from src.logger import Logger
from tempfile import TemporaryDirectory

tmp = TemporaryDirectory()


def test_logger_log():

    logger = Logger(tmp.name, {'key': 'value'})
    logger.log({'name': 'my name', 1: 2})


def test_logger_print():
    logger = Logger(tmp.name, {'key': 'value'})
    logger.info('my message')

    df = logger.to_df()
    assert len(df) > 0
