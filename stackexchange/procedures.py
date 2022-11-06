import sqlite3
from sqlite3 import Error
import click


sql_create_post_id_index = \
    """CREATE UNIQUE INDEX IF NOT EXISTS index_post_id ON posts (post_id, site_id);"""

sql_create_user_id_index = \
    """CREATE UNIQUE INDEX IF NOT EXISTS index_user_id ON users (user_id, site_id);"""

sql_create_post_parent_index = \
    """CREATE INDEX IF NOT EXISTS index_post_parent ON posts (parent_id, site_id);"""

sql_create_post_type_index = \
    """CREATE INDEX IF NOT EXISTS index_post_type ON posts (post_type);"""

sql_select_orphaned_answers = \
    """SELECT post.id FROM posts post
       WHERE post.post_type = 2
       AND NOT EXISTS (SELECT 1 FROM posts other WHERE other.site_id = post.site_id AND other.post_id = post.parent_id);"""

sql_select_orphaned_questions = \
    """SELECT post.id FROM posts post
       WHERE post.accepted_answer_id IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM posts other WHERE other.post_id = post.accepted_answer_id AND other.site_id = post.site_id);"""

sql_update_answer_count = \
    """UPDATE posts AS question
       SET answer_count = (
           SELECT COUNT(answer.id) FROM posts answer
           WHERE answer.site_id = question.site_id
           AND answer.parent_id = question.post_id
       )
       WHERE question.post_type = 1;"""

sql_count_unanswered_questions = \
    """SELECT COUNT(id) FROM posts WHERE post_type = 1 AND answer_count < 1;"""

sql_delete_unanswered_questions = \
    """DELETE FROM posts WHERE post_type = 1 AND answer_count < 1;"""

sql_count_bad_answers = \
    """SELECT COUNT(id) FROM posts WHERE post_type = 2 AND score < 1;"""

sql_delete_bad_answers = \
    """DELETE FROM posts WHERE post_type = 2 AND score < 1;"""

# Delete all posts from users with reputation < 100 which are not accepted answers
sql_select_posts_with_bad_users = \
    """SELECT COUNT(post.id) FROM posts post
       WHERE post.user_id IN (
           SELECT user.user_id FROM users user
           WHERE user.site_id = post.site_id AND user.reputation < 100
       )
       AND NOT EXISTS (
           SELECT 1 FROM posts other
           WHERE other.accepted_answer_id = post.post_id
           AND other.site_id = post.site_id
       );"""

sql_vacuum = \
    """VACUUM"""


def create_indices(db):
    print("Creating user index")
    db.execute(sql_create_user_id_index)
    print("Creating post index")
    db.execute(sql_create_post_id_index)
    db.execute(sql_create_post_parent_index)
    db.execute(sql_create_post_type_index)

def clean_orphaned_questions(db):
    print("Finding questions whose accepted answer does not exist")
    cur = db.cursor()
    cur.execute(sql_select_orphaned_questions)
    post_ids = [row[0] for row in cur.fetchall()]

    # if not click.confirm(f"Clean {len(post_ids)} orphaned questions?", default=False):
    #     print("Skipping")
    #     return

    cur = db.execute(f"UPDATE posts SET accepted_answer_id = NULL WHERE id IN ({sql_select_orphaned_questions[:-1]});")
    print(f"Updated {cur.rowcount} posts")
    db.commit()


def delete_orphaned_answers(db):
    print("Finding answers whose parent question does not exist")
    cur = db.execute(sql_select_orphaned_answers)
    post_ids = [row[0] for row in cur.fetchall()]

    # if not click.confirm(f"Delete {len(post_ids)} orphaned answers?", default=False):
    #     print("Skipping")
    #     return

    cur = db.execute(f"DELETE FROM posts WHERE id IN ({sql_select_orphaned_answers[:-1]});")
    print(f"Deleted {cur.rowcount} posts")
    db.commit()


def delete_unanswered_questions(db):
    print("Finding unanswered questions")
    cur = db.execute(sql_count_unanswered_questions)
    count = cur.fetchone()[0]

    # if not click.confirm(f"Delete {count} unanswered questions?", default=False):
    #     print("Skipping")
    #     return

    cur = db.execute(sql_delete_unanswered_questions)
    print(f"Deleted {cur.rowcount} posts")
    db.commit()


def delete_bad_answers(db):
    print("Finding bad answers")
    cur = db.execute(sql_count_bad_answers)
    count = cur.fetchone()[0]

    # if not click.confirm(f"Delete {count} bad answers?", default=False):
    #     print("Skipping")
    #     return

    cur = db.execute(sql_delete_bad_answers)
    print(f"Deleted {cur.rowcount} posts")
    db.commit()


def update_answer_count(db):
    print("Updating answer count column")
    cur = db.execute(sql_update_answer_count)
    print(f"Updated {cur.rowcount} posts")
    db.commit()


def tidy_database(db_path, interactive=True):
    assert interactive, "Non-interactive mode not supported"

    try:
        db = sqlite3.connect(db_path)
    except Error as e:
        print("An error occured while connecting to database:")
        print(e)
        return

    create_indices(db)

    print("Running clean_orphaned_questions")
    clean_orphaned_questions(db)

    print("Running delete_orphaned_answers")
    delete_orphaned_answers(db)

    print("Running delete_bad_answers")
    delete_bad_answers(db)

    print("Running update_answer_count")
    update_answer_count(db)

    print("Running delete_unanswered_questions")
    delete_unanswered_questions(db)

    print("Running vacuum")
    db.execute("VACUUM;")
    db.commit()

    print("================")
    print("done")
    db.close()
