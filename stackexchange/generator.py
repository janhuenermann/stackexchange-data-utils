import sqlite3
from sqlite3 import Error
import click
from tqdm import tqdm


def generate(db_path):
    try:
        db = sqlite3.connect(db_path)
    except Error as e:
        print("An error occured while connecting to database:")
        print(e)
        return

    total = db.execute("SELECT COUNT(id) FROM posts WHERE post_type = 2;").fetchone()[0]

    cur = db.cursor()
    cur.execute("""

        SELECT question.body, answer.body FROM posts answer
        INNER JOIN posts AS question ON question.post_id = answer.parent_id AND question.site_id = answer.site_id
        WHERE answer.post_type = 2;

        """)

    for item in tqdm(cur, total=total):
        pass

    cur.close()

    print("bye")