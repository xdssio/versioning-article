import contextlib
import os
import os.path as path
import shutil
import pandas as pd
import random
import numpy as np
from random import choice, randint
import string
import tqdm
import duckdb
from faker import Faker
import csv

from pydantic import typing


NYC_TLC_SITE = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
HFVHFV_PATTERN = r'fhvhv_tripdata_'
DOWNLOAD_CHOICES = ['all', '2023', '2022', '2021', '2020', '2019']
MERGED_FILENAME = 'merged.parquet'


class DataFrameGenerator:

    def __init__(self, num_rows: int = 10, file_count: int = 1, seed=1,form='t'):
        self.fake = Faker()
        self.seed = seed
        self.num_rows = num_rows
        self.file_count = file_count
        assert(form == 't' or form == 'n')
        self.form = form

    def generate_data(self, num_rows: int = None):
        num_rows = num_rows or self.num_rows
        headers = ['Name', 'Email', 'Phone', 'Address', 'City', 'State', 
                   'Zip', 'Country', 'Company', 'Job Title',
                   'SSN', 'Latitude', 'Longitude']
        data = []
        if self.form == 't':
            Faker.seed(self.seed)
            for _ in range(num_rows):
                data.append([self.fake.name(),
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
                             self.fake.longitude()])
        else:
            np.random.seed(self.seed)
            data = np.random.rand(num_rows, 10)
            return pd.DataFrame(data)
        print(len(data))
        return pd.DataFrame(data, columns=headers)

    @staticmethod
    def to_parquet(df: pd.DataFrame, filename: str):
        df.to_parquet(filename)

    def generate_mock_files(self, target: str, file_count: int = None, num_rows: int = None):
        Faker.seed(0)
        num_rows = num_rows or self.num_rows
        file_count = file_count or self.file_count

        for filename in tqdm.tqdm(range(0, file_count)):
            self.generate_data(num_rows).to_parquet(f"{target}/{filename}.parquet")


    def generate_file(self, target: str,  num_rows: int = None):
        Faker.seed(0)
        num_rows = num_rows or self.num_rows

        self.generate_data(num_rows).to_parquet(f"{target}")


    def generate_file_csv(self, target: str,  num_rows: int = None):
        Faker.seed(0)
        num_rows = num_rows or self.num_rows

        self.generate_data(num_rows).to_csv(f"{target}")


class NumericDataGenerator:
    def __init__(self, cols: int):
        self.cols = cols

    def generate_data(self, rows: int = None, cols: int = None):
        if cols:
            self.cols = cols
        np.random.seed(1)
        data = np.random.rand(rows, self.cols)
        return pd.DataFrame(data)

    def append(self, filename: str, rows: int = None):
        data = np.random.rand(rows, self.cols)
        df = pd.DataFrame(data)
        if path.exists(filename) and filename.endswith('.parquet'):
            former = pd.read_parquet(filename)
            df = pd.concat([former, df])
            df.to_parquet(filename, engine='pyarrow')
        else:
            df.to_csv(filename, mode='a', header=False, index=False)

    def export(self, df, filepath):
        directory_path = os.path.dirname(filepath)
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)
        os.makedirs(directory_path)
        if filepath.endswith('.parquet'):
            df.to_parquet(filepath, engine='pyarrow')
        else:
            df.to_csv(filepath, index=False)


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

    def generate_data(self, rows: int = 1):
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
