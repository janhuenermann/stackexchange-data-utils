import sqlite3
from sqlite3 import Error
from tqdm import tqdm
import os
import html
from pathlib import Path
from stackexchange.schema import users, posts


def preprocess_post(row, site_id):
    row["site_id"] = site_id
    if row["body"] is not None:
        row["body"] = html.unescape(row["body"])
        row["body"] = row["body"].strip()
    return row


def import_into_database(root_dir, out_path, ignore_meta=False):
    try:
        db = sqlite3.connect(out_path)
    except Error as e:
        print("An error occured while connecting to database:")
        print(e)
        return

    print("Creating database schema...")
    for table in (users, posts):
        table.create_if_not_exists(db)

    print("Inserting posts and users into database....")

    sites_existing = []
    sites_inserted = []

    site_todo = [path for path in Path(root_dir).iterdir() if path.is_dir()]

    with tqdm(site_todo) as pbar:
        for site_id, site_path in enumerate(site_todo):
            site_url = site_path.name
            pbar.set_description(site_url)

            site_dir = str(site_path)
            existing_count = db.execute("SELECT COUNT(*) from users where site_id=?", (site_id,)).fetchone()[0]
            if existing_count > 0:
                sites_existing.append(site_url)
                continue

            def filter_post(row):
                if row["post_type"] not in (1, 2):
                    return None
                return preprocess_post(row, site_id)
            def filter_user(row):
                row["site_id"] = site_id
                return row

            # Users
            users.insert_from_xml(db, os.path.join(site_dir, "Users.xml"), filter_row=filter_user, description="Users")
            # First insert answers
            posts.insert_from_xml(db, os.path.join(site_dir, "Posts.xml"), filter_row=filter_post, description="Posts")

            sites_inserted.append(site_url)

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
