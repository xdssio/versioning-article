import pathlib
import psutil
import typing
from datetime import datetime as dt
import pandas as pd
import os
import time
from loguru import logger
import json
import duckdb
import pyxet


class MetricsHelper:

    def __init__(self):
        self.data_dir = ''
        self.file_count = -1
        self.datetime = dt.now().strftime("%d/%m/%Y %H:%M:%S")
        self.steps = []
        self.filename = ''
        self.file_bytes = 0
        self.step = -1
        self.con = duckdb.connect()
        self._id = hash(self.datetime)
        self.row_count = -1
        self.output = 'output/results.csv'
        self.is_merged = False
        self.pyxet_version = pyxet.__version__

    def count(self, filepath):
        return self.con.execute(f"""SELECT COUNT(*) FROM '{filepath}'""").fetchall()[0][0]

    def get_file_size(self, filepath):
        return self.to_mb(os.path.getsize(filepath))

    @staticmethod
    def to_mb(bytes: int):
        return bytes / (1024 * 1024)

    def set_file(self, filepath: str, step: int, file_bytes: int, is_merged: bool = False):
        logger.info(f"Setting file {filepath} for step {step}")
        self.filename = os.path.basename(filepath)
        self.is_merged = is_merged
        self.file_bytes = file_bytes
        self.row_count = self.count(filepath)
        self.step = step

    def _to_send_recv(self, net_io_before, net_io_after):
        sleep_bytes_sent = net_io_after.bytes_sent - net_io_before.bytes_sent
        sleep_bytes_recv = net_io_after.bytes_recv - net_io_before.bytes_recv
        return self.to_mb(sleep_bytes_sent), self.to_mb(sleep_bytes_recv)

    def bytes_in_rest(self, duration: float):
        net_io_before_sleep = psutil.net_io_counters()
        time.sleep(duration)
        net_io_after_sleep = psutil.net_io_counters()
        return self._to_send_recv(net_io_before_sleep, net_io_after_sleep)

    def record(self, func: typing.Callable, tech: str, *args, **kwargs, ):
        try:
            if self.step == -1:
                raise RuntimeError("No file set")
            error = ''
            start_func_time = time.time()
            net_io_before = psutil.net_io_counters()
            logger.debug(f"Running {func.__name__} with {args} and {kwargs}")
            try:
                func(*args, **kwargs)
            except Exception as e:
                error = str(e)
                logger.error(f"Error running {func.__name__} with {args} and {kwargs} - {e}")

            net_io_after = psutil.net_io_counters()
            func_time = time.time() - start_func_time
            bytes_sent, bytes_recv = self._to_send_recv(net_io_before, net_io_after)
            sleep_bytes_sent, sleep_bytes_recv = self.bytes_in_rest(1)
            result = {'time': func_time,
                      'function': func.__name__,
                      'tech': tech,
                      'merged': self.is_merged,
                      'filename': self.filename,
                      'step': self.step,
                      'file_mb': self.file_bytes,
                      'row_count': self.row_count,
                      'sent_mb': bytes_sent,
                      'recv_mb': bytes_recv,
                      'sent_mb_1s': sleep_bytes_sent,
                      'recv_mb_1s': sleep_bytes_recv,
                      'error': error,
                      }
            logger.debug(json.dumps(result, indent=4))
            self.steps.append(result)
        except KeyboardInterrupt:
            return True
        return False

    def _get_stats(self):
        df = pd.DataFrame(self.steps)
        df['data_dir'] = self.data_dir
        df['file_count'] = self.file_count
        df['datetime'] = self.datetime
        df['pyxet_version'] = self.pyxet_version
        df['id'] = self._id
        return df

    def export(self, verbose: bool = False):
        pathlib.Path(self.output).parent.mkdir(parents=True, exist_ok=True)
        df = self._get_stats()
        if verbose:
            logger.info(df.head())
        df.to_csv(self.output, index=False)
