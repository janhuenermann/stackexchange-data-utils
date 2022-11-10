from concurrent.futures import ProcessPoolExecutor
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
    # Convert bold/italic to markdown
    for bold_tag in ("strong", "b"):
        for bold in soup.find_all(bold_tag):
            if bold.string is None:
                continue
            bold.string.replace_with(f"**{bold.string}**")
    for italic_tag in ("em", "i"):
        for italic in soup.find_all(italic_tag):
            if italic.string is None:
                continue
            italic.string.replace_with(f"*{italic.string}*")
    # Mark links
    for link in soup.find_all("a"):
        if link.string is None:
            continue
        href = link.get("href")
        if href is None:
            continue
        link.string.replace_with(f"[{link.string}]()")
    return soup.get_text(separator=" ", strip=True)


def generate(
    db_path,
    out_dir,
    tags=None,
    min_answer_score=3,         # all answers need to have at least this score
    min_extra_answer_score=10,  # include extra answers with score >= min_extra_answer_score
    max_answer_length=4096,
    max_chunk_size=100_000_000, # each file should have max this many bytes
):
    try:
        db = sqlite3.connect(db_path)
    except Error as e:
        print("An error occured while connecting to database:")
        print(e)
        return

    cur = db.cursor()
    where_queries = [
        f"answer.post_type = 2",
        f"answer.score >= {min_answer_score}",
        f"(question.accepted_answer_id = answer.post_id OR answer.score >= {min_extra_answer_score})",
        f"length(answer.body) < {max_answer_length}",
    ]

    if tags is not None:
        for tag in tags:
            where_queries.append(f"question.tags like '%<{tag}>%'")

    where_query = " AND ".join(where_queries)
    print(where_query)
    total = db.execute(f"""
        SELECT COUNT(question.id) FROM posts answer
        INNER JOIN posts AS question ON answer.parent_id = question.post_id AND question.site_id = answer.site_id
        WHERE {where_query};""").fetchone()[0]

    cur.execute(f"""
        SELECT question.title, question.tags, question.body, answer.body FROM posts answer
        INNER JOIN posts AS question ON answer.parent_id = question.post_id AND question.site_id = answer.site_id
        WHERE {where_query} ORDER BY RANDOM();""")

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

    pool = ProcessPoolExecutor(max_workers=16)
    for line in tqdm(pool.map(process_line, cur, chunksize=8), total=total, ncols=120):
        if line is None:
            continue
        fd.write(line.encode(encoding, errors="ignore"))
        if fd.tell() > max_chunk_size:
            next_chunk()

    pool.shutdown()
    cur.close()
    print("bye")


def process_line(item):
    question_title, question_tags, question, answer = item
    try:
        question_body = html_to_plaintext(question).replace("\n", " ").strip()
        answer_body = html_to_plaintext(answer).replace("\n", " ").strip()
        question_tags = question_tags.strip().replace("><", ", ").replace("<", "").replace(">", "")
        question_title = question_title.strip()
        if len(question_body) > 512:
            # If the question is too long, we don't want to include it
            line = f"<TITLE> {question_title} <TAGS> {question_tags} <ANSWER> {answer_body}\n"
        else:
            line = f"<TITLE> {question_title} <BODY> {question_body} <TAGS> {question_tags} <ANSWER> {answer_body}\n"
    except KeyboardInterrupt:
        return None
    except Exception as e:
        print(f"Skipped question answer pair due to error: {e}")
        return None
    return line
