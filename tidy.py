import argparse
from stackexchange.procedures import tidy_database


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('db', type=str, help='Path to database in which to store dataset')

options = {
   "delete_orphaned_questions": "Delete questions whose accepted answer does not exist",
   "delete_unanswered_questions": "Delete questions with no answers",
   "delete_bad_answers": "Delete answers with score < 1",
}

defaults = dict()
for option, desc in options.items():
   parser.add_argument(f"--{option.replace('_', '-')}", dest=option, action="store_true", help=desc)
   defaults[option] = False

args = parser.parse_args()
print(args)
#tidy_database(args.db)
