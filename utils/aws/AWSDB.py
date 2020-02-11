from utils.aws import MySQLDB

conn_aws = MySQLDB.get_connection()


def setup_schema():

    MySQLDB.setup_tables(conn_aws)


def insert_reddit_posts(posts):

    MySQLDB.insert_reddit_posts(conn_aws, posts)


def update_reddit_posts_batch_id(names, batch_id):

    MySQLDB.update_reddit_posts_batch_id(conn_aws, names, batch_id)


def insert_nlp_posts(posts):

    MySQLDB.insert_nlp_posts(conn_aws, posts)


def get_next_reddit_batch_id():

    return MySQLDB.get_next_reddit_batch_id(conn_aws)


def get_reddit_posts(subreddit):

    return MySQLDB.get_reddit_posts(conn_aws, subreddit)


def get_reddit_post_names():

    return MySQLDB.get_reddit_post_names(conn_aws)


def get_reddit_posts_by_batch(batch_id):

    return MySQLDB.get_reddit_posts_by_batch(conn_aws, batch_id)


def get_reddit_posts_for_nlp(limit=0):

    return MySQLDB.get_reddit_posts_for_nlp(conn_aws, limit)


def get_nlp_post(name):

    return MySQLDB.get_nlp_post(conn_aws, name)


def get_all_nlp_posts():

    return MySQLDB.get_all_nlp_posts(conn_aws)

def get_next_nlp_batch_id():

    return MySQLDB.get_next_nlp_batch_id(conn_aws)


