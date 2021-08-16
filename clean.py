# Delete all posts where the accepted answer got lost
delete_orphaned_answers = """
DELETE FROM posts post WHERE post.accepted_answer_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM posts other WHERE post.accepted_answer_id = other.id)
"""

# Delete all posts from users with reputation < 100 which are not accepted answers
delete_bad_posts = """
DELETE FROM posts post WHERE post.user_id IN (SELECT user.user_id FROM users user WHERE user.reputation < 100) AND NOT EXISTS (SELECT 1 FROM posts other WHERE other.accepted_answer_id = post.id)
"""

vacuum = """
VACUUM
"""