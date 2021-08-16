import argparse
from stackexchange.procedures import tidy_database


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('db', type=str, help='Path to database in which to store dataset')

args = parser.parse_args()
tidy_database(args.db, interactive=True)
