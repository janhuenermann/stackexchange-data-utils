import sqlite3
from sqlite3 import Error


create_post_id_index = \
    """CREATE UNIQUE INDEX IF NOT EXISTS index_post_id ON posts (post_id, site_id);"""

create_user_id_index = \
    """CREATE UNIQUE INDEX IF NOT EXISTS index_user_id ON users (user_id, site_id);"""

select_orphaned_questions = \
    """SELECT post.id FROM posts post
       WHERE post.accepted_answer_id IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM posts other WHERE other.post_id = post.accepted_answer_id AND other.site_id = post.site_id);"""

select_bad_answers = \
    """SELECT COUNT(*) FROM posts post WHERE post.post_type = 2 AND post.score < 1"""

# Delete all posts from users with reputation < 100 which are not accepted answers
select_bad_users = \
    """SELECT COUNT(*) FROM posts post WHERE post.user_id IN (SELECT user.user_id FROM users user WHERE user.site_id = post.site_id AND user.reputation < 100) AND NOT EXISTS (SELECT 1 FROM posts other WHERE other.accepted_answer_id = post.post_id AND other.site_id = post.site_id);"""

vacuum = \
    """VACUUM"""


def create_indices(db):
    print("Creating user index")
    db.execute(create_user_id_index)
    print("Creating post index")
    db.execute(create_post_id_index)


def delete_orphaned_questions(db):
    print("Finding orphaned questions (whose answer does not exist)")

    cur = db.cursor()
    cur.execute(select_orphaned_questions)
    post_ids = cur.fetchall()

    if len(post_ids) > 1000:
        print("WARNING : found more than 10^3 orphaned questions. exit.")
        exit(1)
        return

    print
    print(post_ids)
    print(len(post_ids))


def tidy_database(db_path, args):
    try:
        db = sqlite3.connect(db_path)
    except Error as e:
        print("An error occured while connecting to database:")
        print(e)
        return

    create_indices(db)
    delete_orphaned_questions(db)

    db.close()
