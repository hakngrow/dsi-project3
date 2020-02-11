from utils.aws import MySQLDB

conn_aws = MySQLDB.get_connection()


def setup_schema():

    MySQLDB.setup_tables(conn_aws)


def insert_reddit_posts(posts):

    MySQLDB.insert_reddit_posts(conn_aws, posts)


def update_nlp_posts(posts):

    MySQLDB.update_nlp_posts(conn_aws, posts)


def get_next_reddit_batch_id():

    return MySQLDB.get_next_reddit_batch_id(conn_aws)


def get_reddit_post_names():

    return MySQLDB.get_reddit_post_names(conn_aws)


def get_reddit_posts_by_batch(batch_id):

    return MySQLDB.get_reddit_posts_by_batch(conn_aws, batch_id)


def get_reddit_posts_for_nlp():

    return MySQLDB.get_reddit_posts_for_nlp(conn_aws)


def get_nlp_post(name):

    return MySQLDB.get_nlp_post(conn_aws, name)


def get_all_nlp_posts():

    return MySQLDB.get_all_nlp_posts(conn_aws)


