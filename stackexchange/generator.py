import sqlite3
from sqlite3 import Error
import click
from tqdm import tqdm
import os


def generate(db_path, out_dir, max_chunk_size=100_000_000):
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
        WHERE answer.post_type = 2;""")

    chunk_format = "chunk_{:04d}.txt"
    chunk_index = -1
    fd = None

    def next_chunk():
        nonlocal chunk_index
        nonlocal fd
        chunk_index += 1
        chunk_path = os.path.join(out_dir, chunk_format.format(chunk_index))
        if fd is not None:
            fd.close()
        fd = open(chunk_path, "w")

    os.makedirs(out_dir)
    next_chunk()
    endian = "big"

    for item in tqdm(cur, total=total):
        question, answer = item
        qlen = len(question)
        rlen = qlen + len(answer)
        # [row length], [question length], [question string], [answer string]
        fd.write(rlen.to_bytes(4, endian))
        fd.write(qlen.to_bytes(4, endian))
        fd.write(question + answer)
        if fd.tell() > max_chunk_size:
            next_chunk()

    cur.close()

    print("bye")