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


class MetricsHelper:

    def __init__(self, data_dir: str, file_count: int):
        self.data_dir = data_dir
        self.file_count = file_count
        self.datetime = dt.now().strftime("%d/%m/%Y %H:%M:%S")
        self.steps = []
        self.filename = None
        self.step = -1
        self.con = duckdb.connect()
        self._id = hash(self.datetime)

    def count(self, filepath):
        return self.con.execute(f"""SELECT COUNT(*) FROM '{filepath}'""").fetchall()[0][0]

    def set_file(self, filepath: str, step: int):
        self.filename = os.path.basename(filepath)
        self.file_bytes = os.path.getsize(filepath) / (1024 * 1024)  # to MB
        self.row_count = self.count(filepath)
        self.step = step

    def bytes_in_rest(self, duration: float):
        net_io_before_sleep = psutil.net_io_counters()
        time.sleep(duration)
        net_io_after_sleep = psutil.net_io_counters()
        sleep_bytes_sent = net_io_after_sleep.bytes_sent - net_io_before_sleep.bytes_sent
        sleep_bytes_recv = net_io_after_sleep.bytes_recv - net_io_before_sleep.bytes_recv
        return sleep_bytes_sent, sleep_bytes_recv

    def record(self, func: typing.Callable, tech: str, merged: bool = True, *args, **kwargs, ):
        start_func_time = time.time()
        net_io_before = psutil.net_io_counters()
        logger.debug(f"Running {func.__name__} with {args} and {kwargs}")
        func(*args, **kwargs)
        net_io_after = psutil.net_io_counters()
        func_time = time.time() - start_func_time
        bytes_sent = net_io_after.bytes_sent - net_io_before.bytes_sent
        bytes_recv = net_io_after.bytes_recv - net_io_before.bytes_recv
        sleep_bytes_sent, sleep_bytes_recv = self.bytes_in_rest(1)
        result = {'time': func_time,
                  'function': func.__name__,
                  'tech': tech,
                  'merged': merged,
                  'filename': self.filename,
                  'step': self.step,
                  'file_bytes': self.file_bytes,
                  'row_count': self.row_count,
                  'bytes_sent': bytes_sent,
                  'bytes_recv': bytes_recv,
                  'bytes_sent_1s': sleep_bytes_sent,
                  'bytes_recv_1s': sleep_bytes_recv
                  }
        logger.debug(json.dumps(result, indent=4))
        self.steps.append(result)

    def _get_output(self):
        df = pd.DataFrame(self.steps)
        df['data_dir'] = self.data_dir
        df['file_count'] = self.file_count
        df['datetime'] = self.datetime
        df['id'] = self._id
        return df

    def export(self, output: str = 'output.csv', verbose: bool = False):
        pathlib.Path(output).parent.mkdir(parents=True, exist_ok=True)
        df = self._get_output()
        if verbose:
            logger.info(df.head())
        df.to_csv(output, index=False)
