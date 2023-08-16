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

fake = Faker()

NYC_TLC_SITE = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
HFVHFV_PATTERN = r'fhvhv_tripdata_'
DOWNLOAD_CHOICES = ['all', '2023', '2022', '2021', '2020', '2019']
MERGED_FILENAME = 'merged.parquet'


def generate_data(num_rows: int = 10):
    headers = ['Name', 'Email', 'Phone', 'Address', 'City', 'State', 'Zip', 'Country', 'Company', 'Job Title',
               'SSN', 'Latitude', 'Longitude']
    data = []

    for _ in range(num_rows):
        data.append([fake.name(),
                     fake.email(),
                     fake.phone_number(),
                     fake.address(),
                     fake.city(),
                     fake.state(),
                     fake.zipcode(),
                     fake.country(),
                     fake.company(),
                     fake.job(),
                     fake.ssn(),
                     fake.latitude(),
                     fake.longitude()])

    return pd.DataFrame(data, columns=headers)


def save_parquet(df: pd.DataFrame, filename: str):
    df.to_parquet(filename)


def generate_mock_files(target: str, file_count: int = 5, num_rows: int = 10):
    shutil.rmtree(target, ignore_errors=True)
    os.mkdir(target)

    for filename in tqdm.tqdm(range(0, file_count)):
        save_parquet(generate_data(num_rows), f"{target}/{filename}.parquet")


def merge_files(new_filename: str, merged_filename: str):
    if not path.exists(merged_filename):
        shutil.copyfile(new_filename, merged_filename)
    else:
        duckdb.execute(f"""
                    COPY (SELECT * FROM read_parquet(['{new_filename}', '{merged_filename}'])) TO '{merged_filename}' (FORMAT 'parquet');
                    """)
    return merged_filename


class BlogRowsGenerator:
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

    def generate_row(self):
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
        return pd.DataFrame([new_row])

    def append_mock_row(self, filepath):
        row = self.generate_row()
        row.to_csv(filepath, mode='a', header=False, index=False)
