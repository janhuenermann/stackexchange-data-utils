import argparse
from stackexchange.db import import_into_database

parser = argparse.ArgumentParser(description='Extract and ingest Stack Exchange data into sqlite3 database.')
parser.add_argument('root_dir', type=str, help='path to directory containing 7z archives')
parser.add_argument('db', type=str, help='path to database in which to store dataset')
parser.add_argument('--ignore-meta', dest='ignore_meta', action='store_true')
parser.set_defaults(ignore_meta=False)

args = parser.parse_args()
import_into_database(args.root_dir, args.db, ignore_meta=args.ignore_meta)
