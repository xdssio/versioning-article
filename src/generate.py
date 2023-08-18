import argparse
from utils.generators import DataFrameGenerator

if __name__ == '__main__':
    p = argparse.ArgumentParser('Benchmarking of NYC Taxi data in different repositories')
    p.add_argument(
        '--dir', default='data',
        help='The directory in which to save generated data')
    p.add_argument(
        '--count', default=5,
        help='Number of files to generate')
    p.add_argument(
        '--rows', default=1000, type=int,
        help='number of rows per file')
    args = p.parse_args()
    generator = DataFrameGenerator(target=args.dir, rows=args.rows, count=args.count)
    generator.generate_mock_files()

