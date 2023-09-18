import contextlib
import os
import os.path as path
import shutil
import pandas as pd
import numpy as np
from random import choice, randint
import string
import tqdm
from faker import Faker

from pydantic import typing

NYC_TLC_SITE = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
HFVHFV_PATTERN = r'fhvhv_tripdata_'
DOWNLOAD_CHOICES = ['all', '2023', '2022', '2021', '2020', '2019']
MERGED_FILENAME = 'merged.parquet'


class DataFrameGenerator:

    def __init__(self, seed: int = 42, numeric: bool = False):
        self.numeric = numeric
        self.seed = seed
        self.fake = Faker()
        self.columns = ['Name', 'Email', 'Phone', 'Address', 'City', 'State', 'Zip', 'Country', 'Company', 'Job Title',
                        'SSN', 'Latitude', 'Longitude'] if not numeric else ['col_' + str(i) for i in range(0, 10)]

    def generate(self, num_rows: int):
        if not self.numeric:
            Faker.seed(self.seed)
            data = [[self.fake.name(),
                     self.fake.email(),
                     self.fake.phone_number(),
                     self.fake.address(),
                     self.fake.city(),
                     self.fake.state(),
                     self.fake.zipcode(),
                     self.fake.country(),
                     self.fake.company(),
                     self.fake.job(),
                     self.fake.ssn(),
                     self.fake.latitude(),
                     self.fake.longitude()] for _ in range(num_rows)]

        else:
            np.random.seed(self.seed)
            data = np.random.rand(num_rows, len(self.columns))
        return pd.DataFrame(data, columns=self.columns)

    def generate_features(self, num_rows: int, num_columns: int):
        np.random.seed(self.seed)
        data = np.random.rand(num_rows, num_columns)
        return pd.DataFrame(data, columns=[f"feature_{i}" for i in range(num_columns)])

    def export(self, df: pd.DataFrame, filepath: str):
        directory_path = os.path.dirname(filepath)
        os.makedirs(directory_path, exist_ok=True)
        if filepath.endswith('.parquet'):
            df.to_parquet(filepath, engine='pyarrow')
        else:
            df.to_csv(filepath, index=False)

    def generate_mock_files(self, target: str, num_rows: int, file_count: int = 1):
        shutil.rmtree(target, ignore_errors=True)
        os.mkdir(target)
        for filename in tqdm.tqdm(range(0, file_count)):
            self.generate(num_rows).to_parquet(f"{target}/{filename}.parquet")


class BlogDataGenerator:
    def __init__(self,
                 counter: int,
                 genders: typing.List[str],
                 topics: typing.List[str],
                 signs: typing.List[str],
                 date: pd.Timestamp,
                 ):
        self.counter = counter
        self.genders = genders
        self.topics = topics
        self.signs = signs
        self.date = pd.to_datetime('01,January,2004')
        with contextlib.suppress(ValueError):
            self.date = pd.to_datetime(date)

    @staticmethod
    def generate_text():
        return ''.join([choice(string.ascii_letters) for _ in range(100)])

    def generate_gender(self):
        return choice(self.genders)

    def generate_topic(self):
        return choice(self.topics)

    def generate_sign(self):
        return choice(self.signs)

    def generate_age(self):
        return randint(0, 100)

    def generate(self, rows: int = 1):
        data = []
        for i in range(rows):
            new_row = {'id': self.counter,
                       'gender': self.generate_gender(),
                       'age': self.generate_age(),
                       'topic': self.generate_topic(),
                       'signs': self.generate_sign(),
                       'date': self.date.strftime('%d,%B,%Y'),
                       'text': self.generate_text()
                       }
            self.counter += 1
            self.date = self.date + pd.DateOffset(hours=1)
            data.append(new_row)
        return pd.DataFrame(data)

    def append(self, filepath, rows: int = 1):
        row = self.generate_data(rows)
        row.to_csv(filepath, mode='a', header=False, index=False)
