import argparse
from stackexchange.generator import generate

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('db', type=str, help='Path to database in which to store dataset')
args = parser.parse_args()

generate(args.db)