import argparse
from utils.generators import DataFrameGenerator

if __name__ == '__main__':
    p = argparse.ArgumentParser('Benchmarking of NYC Taxi data in different repositories')
    p.add_argument(
        '--path', default='0.csv',
        help='The file to write')
    p.add_argument(
        '--rows', default=1000, type=int,
        help='number of rows per file')
    p.add_argument(
        '--seed', default=1, type=int,
        help='random seed')
    args = p.parse_args()
    generator = DataFrameGenerator(num_rows=args.rows, seed=args.seed)
    if args.path.endswith('.parquet'):
        generator.generate_file(target=args.path)
    else:
        generator.generate_file_csv(target=args.path)

