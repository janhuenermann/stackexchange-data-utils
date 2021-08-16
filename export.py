import argparse
from stackexchange.generator import generate

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('db', type=str, help='Path to database in which to store dataset')
parser.add_argument('out_dir', type=str, help='Path to output directory')
args = parser.parse_args()

generate(args.db, args.out_dir)