import sqlite3
from sqlite3 import Error
import xml.etree.ElementTree as ET
from tqdm import tqdm
import os
from collections import OrderedDict
import re

from stackexchange.tables import sites, users, posts


def filter_post(row):
    return row["post_type"] not in (1, 2)


def import_into_database(root_dir, out_path, ignore_meta=False):
    assert not os.path.exists(out_path)

    try:
        db = sqlite3.connect(out_path)
    except Error as e:
        print("An error occured while connecting to database:")
        print(e)
        return

    print("Creating database schema...")

    for table in (sites, users, posts):
        table.create_if_not_exists(db)

    print("Finding site information...")

    with open(os.path.join(root_dir, "Sites.xml"), "r") as fd:
        def filter_sites(row):
            if ignore_meta:
                meta_in_url = "/meta." in row["url"] or ".meta." in row["url"]
                meta_in_name = "meta" in row["name"].lower()
                return meta_in_name or meta_in_url
            return False
        sites.insert_from_xml(db, fd, filter_sites)

    site_cur = db.cursor()
    site_cur.execute("SELECT (id, url) FROM sites")
    site_todo = site_cur.fetchall()

    print("Inserting posts and users into database....")

    with tqdm(site_todo) as pbar:
        for site in pbar:
            site_id = int(site[0])
            site_url = re.match("https?://(.+)", site[1]).group(1)
            pbar.set_description(site_url)

            site_dir = os.path.join(root_dir, site_url)

            if not os.path.isdir(site_dir):
                print(f"WARNING : could not find directory for site {site_url}.")
                continue

            existing_count = db.execute("SELECT COUNT(*) from users where site_id=?", (site_id,))[0]
            if existing_count > 0:
                print(f"WARNING : found existing entries for {site_url}, skipping.")
                continue

            def modify_post(row):
                row["site_id"] = site_id
                if filter_post(row):
                    return None
                return row

            def modify_user(row):
                row["site_id"] = site_id

            users.insert_from_xml(db, os.path.join(site_dir, "Users.xml"), modify_row=modify_user)
            posts.insert_from_xml(db, os.path.join(site_dir, "Posts.xml"), modify_row=modify_post)

    db.close()
