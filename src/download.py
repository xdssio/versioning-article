import argparse

from utils import DOWNLOAD_CHOICES, download

if __name__ == '__main__':
    p = argparse.ArgumentParser('Benchmarking of NYC Taxi data in different repositories')
    p.add_argument(
        '--dir', default='data',
        help='The directory in which to download data and perform searches.')
    p.add_argument(
        '--download', default='all',
        choices=DOWNLOAD_CHOICES,
        help='Download data from these years'
             'Choices: {}'.format(', '.join(DOWNLOAD_CHOICES)))
    p.add_argument(
        '--limit', default=-1, type=int,
        help='max number of files to download')
    args = p.parse_args()
    download(args.dir, args.download, args.limit)
