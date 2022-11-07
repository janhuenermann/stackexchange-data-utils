import sqlite3
from sqlite3 import Error
from tqdm import tqdm
import os
from bs4 import BeautifulSoup


def html_to_plaintext(html: str):
    soup = BeautifulSoup(html)
    # Annotate code
    for code in soup.find_all("code"):
        plain_text_code = code.string
        if plain_text_code is None:
            continue
        # Replace tab with 2 spaces
        plain_text_code = plain_text_code.replace("\t", "  ")
        # Replace newlines with tabs
        plain_text_code = plain_text_code.replace("\n", "\t")
        plain_text_code = plain_text_code.strip("\t ")
        if len(plain_text_code) == 0:
            code.decompose()
            continue
        code.string.replace_with(f"`{plain_text_code}`")
    return soup.get_text(separator=" ", strip=True)


def generate(db_path, out_dir, tags=None, max_question_length=1000, max_chunk_size=100_000_000, include_title_and_tags=True):
    try:
        db = sqlite3.connect(db_path)
    except Error as e:
        print("An error occured while connecting to database:")
        print(e)
        return

    cur = db.cursor()
    tag_query = ""
    if tags is not None:
        tag_query = " AND (" + " OR ".join([f"question.tags LIKE '%{tag}%'" for tag in tags]) + ")"

    where_query = f"answer.post_type = 2 AND length(question.body) < {max_question_length}{tag_query}"
    total = db.execute(f"""
        SELECT COUNT(question.id) FROM posts answer
        INNER JOIN posts AS question ON question.post_id = answer.parent_id AND question.site_id = answer.site_id
        WHERE {where_query};""").fetchone()[0]

    cur.execute(f"""
        SELECT question.title, question.tags, question.body, answer.body FROM posts answer
        INNER JOIN posts AS question ON question.post_id = answer.parent_id AND question.site_id = answer.site_id
        WHERE {where_query};""")

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
        fd = open(chunk_path, "wb")

    os.makedirs(out_dir, exist_ok=True)
    next_chunk()
    encoding = "utf-8"

    for item in tqdm(cur, total=total):
        question_title, question_tags, question, answer = item

        try:
            question = html_to_plaintext(question).replace("\n", " ").strip()
            if include_title_and_tags:
                question_tags = question_tags.strip().replace("><", ", ").replace("<", "").replace(">", "")
                question_title = question_title.strip()
                question = f"{question} <TAGS> {question_tags} <SUMMARY> {question_title}"
            answer = html_to_plaintext(answer).replace("\n", " ").strip()
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Skipped question answer pair due to error: {e}")
            continue

        line = question + " <ANSWER> " + answer + "\n"
        fd.write(line.encode(encoding, errors="ignore"))
        if fd.tell() > max_chunk_size:
            next_chunk()

    cur.close()
    print("bye")
