import argparse
from stackexchange.generator import generate

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('db', type=str, help='Path to database in which to store dataset')
parser.add_argument('out_dir', type=str, help='Path to output directory')
parser.add_argument('--tags', default=None, type=str, help='Only export posts with these tags. Comma-separated list.')
args = parser.parse_args()

tags = [tag.strip() for tag in args.tags.split(',')] if args.tags else None
generate(args.db, args.out_dir, tags)
