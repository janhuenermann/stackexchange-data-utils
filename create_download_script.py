import requests
import re
from datetime import datetime

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

script = "\n".join([f"wget -O {url} {base_url}/{url}" for url in urls])
script = f"""#!/bin/bash

# Generated on {now.strftime("%d/%m/%Y %H:%M:%S")}

{script}
"""

f = open("download.sh", "w")
f.write(script)
f.close()

print("Wrote script to `download.sh`")