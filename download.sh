#!/bin/bash
mkdir -p ./data

# Download links can be found at: https://archive.org/details/stackexchange
DOWNLOAD_LINKS=(
    https://ia600107.us.archive.org/27/items/stackexchange/superuser.com.7z
    https://ia800107.us.archive.org/27/items/stackexchange/serverfault.com.7z
)

# Download archives to data/ if they don't already exist
for link in "${DOWNLOAD_LINKS[@]}"; do
    filename=$(basename "$link")
    if [ ! -f "data/$filename" ]; then
        wget -O "data/$filename" "$link"
    fi
done

# Loop over each archive and unzip it using 7z if it's not already unzipped
for archive in data/*.7z; do
    if [ ! -d "${archive%.7z}" ]; then
        7z x "$archive" -o"${archive%.7z}"
    fi
done

# Uncomment if you want the big stackoverflow dump
# mkdir -p data/stackoverflow.com/
# wget -O data/stackoverflow.com-Users.7z https://ia600107.us.archive.org/27/items/stackexchange/stackoverflow.com-Users.7z
# wget -O data/stackoverflow.com-Posts.7z https://ia600107.us.archive.org/27/items/stackexchange/stackoverflow.com-Posts.7z
# 7z x data/stackoverflow.com-Users.7z -o data/stackoverflow.com/
# 7z x data/stackoverflow.com-Posts.7z -o data/stackoverflow.com/

echo "DONE! :)"
