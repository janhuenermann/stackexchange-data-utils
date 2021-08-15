import argparse
import os
import subprocess
from tqdm import tqdm
import re
import shutil

from stackexchange.process import import_into_database

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('root_dir', type=str, help='path to directory containing the 7z archives')
parser.add_argument('db', type=str, help='path to database in which to store dataset')
parser.add_argument('--ignore-meta', dest='ignore_meta', action='store_true')
parser.set_defaults(ignore_meta=False)

args = parser.parse_args()

partfile_pattern = "(.+\.com)-(\w+)$"
files = [name[:-3] for name in os.listdir(args.root_dir) if name.endswith(".7z")]

print(f"Extracting {len(files)} archives...")
with tqdm(files) as pbar:
   for file in pbar:
      pbar.set_description(file)
      archive_path = os.path.join(args.root_dir, f"{file}.7z")
      file_dir = os.path.join(args.root_dir, file)
      partfile = re.match(partfile_pattern, file)
      if partfile is not None:
          # Skipping since all partfiles have been extracted
          if os.path.isdir(os.path.join(args.root_dir, partfile.group(1))):
              continue
      # Skipping since file has already been extracted
      if os.path.isdir(file_dir):
         continue
      os.makedirs(file_dir, exist_ok=False)
      process = subprocess.run(["7z", "x", f"-o{file_dir}", archive_path], capture_output=True)
      assert process.returncode == 0, process.stderr
      # os.remove(archive_path)

print("Merging part files...")

partfiles = [re.match(partfile_pattern, file) for file in files]
partfiles = [file for file in partfiles if file is not None]
sites_of_partfiles = set([file.group(1) for file in partfiles])

for partfile in partfiles:
   file = partfile.group(0)
   site = partfile.group(1)
   file_dir = os.path.join(args.root_dir, file)
   site_dir = os.path.join(args.root_dir, site)
   os.makedirs(site_dir, exist_ok=True)
   if not os.path.isdir(file_dir):
      continue
   for name in os.listdir(file_dir):
      shutil.move(os.path.join(file_dir, name), site_dir)
   os.rmdir(file_dir)

print("Creating database...")
import_into_database(args.root_dir, args.db, ignore_meta=args.ignore_meta)
