import sqlite3
from sqlite3 import Error
import xml.etree.ElementTree as ET
from tqdm import tqdm
import os
from collections import OrderedDict
import re

from stackexchange.tables import sites, users, posts


def import_into_database(root_dir, out_path, ignore_meta=False):
    # assert not os.path.exists(out_path)

    try:
        db = sqlite3.connect(out_path)
    except Error as e:
        print("An error occured while connecting to database:")
        print(e)
        return

    # Turn on foreign key constraints
    db.execute("PRAGMA foreign_keys = ON;")

    print("Creating database schema...")
    for table in (sites, users, posts):
        table.create_if_not_exists(db)



    existing_site_count = db.execute("SELECT COUNT(*) from sites").fetchone()[0]

    print("Inserting sites...")

    def filter_sites(row):
        if ignore_meta:
            meta_in_url = "/meta." in row["url"] or ".meta." in row["url"]
            meta_in_name = "meta" in row["name"].lower()
            if meta_in_name or meta_in_url:
                return None
        return row

    sites.insert_from_xml(db, os.path.join(root_dir, "Sites.xml"), filter_row=filter_sites)

    site_cur = db.cursor()
    site_cur.execute("SELECT id, url FROM sites")
    site_todo = site_cur.fetchall()

    print("Inserting posts and users into database....")

    sites_not_found = []
    sites_existing = []
    sites_inserted = []

    with tqdm(site_todo) as pbar:
        for site in pbar:
            site_id = int(site[0])
            site_url = re.match("https?://(.+)", site[1]).group(1)
            pbar.set_description(site_url)

            site_dir = os.path.join(root_dir, site_url)
            if not os.path.isdir(site_dir):
                sites_not_found.append(site_url)
                continue

            existing_count = db.execute("SELECT COUNT(*) from users where site_id=?", (site_id,)).fetchone()[0]
            if existing_count > 0:
                sites_existing.append(site_url)
                continue

            def filter_question(row):
                if row["post_type"] != 1:
                    return None
                row["site_id"] = site_id
                return row

            def filter_answer(row):
                if row["post_type"] != 2:
                    return None
                row["site_id"] = site_id
                return row

            def filter_user(row):
                row["site_id"] = site_id
                return row

            # Users
            users.insert_from_xml(db, os.path.join(site_dir, "Users.xml"), filter_row=filter_user, description="Users")
            # First insert answers
            posts.insert_from_xml(db, os.path.join(site_dir, "Posts.xml"), filter_row=filter_answer, description="Answers")
            # Then insert questions
            posts.insert_from_xml(db, os.path.join(site_dir, "Posts.xml"), filter_row=filter_question, description="Questions")

            sites_inserted.append(site_url)

    if len(sites_not_found) > 0:
        print("Sites not found:")
        print("================")
        for site in sites_not_found:
            print(site)
        print("================")

    if len(sites_existing) > 0:
        print("Sites existing:")
        print("===============")
        for site in sites_existing:
            print(site)
        print("===============")

    if len(sites_inserted) > 0:
        print("Sites inserted:")
        print("===============")
        for site in sites_inserted:
            print(site)
        print("===============")

    db.close()
