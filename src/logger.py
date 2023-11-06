from loguru import logger
import os
from datetime import datetime
import json
from json import JSONDecodeError
import pandas as pd
from glob import glob
import contextlib
from datetime import datetime
from coolname import generate_slug


class Logger():

    def __init__(self, path: str = 'logs', params: dict = None):
        """
        path: path to save the log files
        params: dict with params to add to each track call
        """
        if params is None:
            params = {}
        os.makedirs(path, exist_ok=True)
        self.name = generate_slug(3)
        params['run_name'] = self.name
        self.path = path
        self.logger = logger
        self.params = params
        self.name = generate_slug(3)
        log_file = "{time:YYYY-MM-DD}.log"
        logger.add(f"{path}/{log_file}", rotation="1 day")

    def log(self, params):
        params['timestamp'] = datetime.now().isoformat()
        self.logger.debug(f"|{json.dumps({**self.params, **params})}")

    def info(self, message):
        self.logger.info(message)

    def to_df(self, latest: bool = True):
        """
        Returnes a dataframe constructed from the log files
        latest: if True, only the latest log file is used
        """
        files = glob(f"{self.path}/*.log")
        if latest:
            files = [max(files, key=lambda x: os.path.getctime(x))]
        records = []
        for file in files:
            with open(file, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    sections = line.split('|')
                    if len(sections) > 1 and 'DEBUG' in sections[1]:
                        with contextlib.suppress(JSONDecodeError):
                            records.append(json.loads(sections[-1]))
        return pd.DataFrame(records)
