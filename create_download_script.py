import requests
import re
from datetime import datetime

print("**** Note ****")
print("If you want to download the whole stackexchange dataset,")
print("you are probably better of downloading using torrent:")
print("https://archive.org/download/stackexchange/stackexchange_archive.torrent")
print("**** ****")
print("")

base_url = """https://archive.org/download/stackexchange"""
response = requests.get(base_url)
body = response.text
matches = re.findall("""td><a\s+href="([^\"]+)"\s*>[^<]+<\/\s*a\s*>\s\(<\s*a\s+href="([^\"]+)"\s*>View Contents<\/\s*a\s*>\)<\/\s*td\s*>""", body)

urls = [m[0] for m in matches]
print(f"Found {len(urls)} files")
# Remove all meta sites
urls = [url for url in urls if ".meta." not in url]
print(f"Collected {len(urls)} files without meta sites")

now = datetime.now()

for url in urls:
   assert url.endswith(".7z")

files = [f"'{url[:-3]}'" for url in urls]
script = f"""#!/bin/bash

# Generated on {now.strftime("%d/%m/%Y %H:%M:%S")}

FILES=({" ".join(files)})
BASE_URL="{base_url}"

for FILE in "${{FILES[@]}}"
do

wget -O $FILE.7z $BASE_URL/$FILE.7z
(mkdir $FILE && cd $FILE && 7z x ../$FILE.7z)
rm $FILE.7z

done
"""

f = open("download.sh", "w")
f.write(script)
f.close()

print("Wrote script to `download.sh`")