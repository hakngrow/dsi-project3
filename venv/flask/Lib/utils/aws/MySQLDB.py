from mysql.connector import connect

from utils.model.post import RedditPost, NLPPost

TBL_POSTS_REDDIT = 'posts'

COL_NAME = 'name'
COL_SUBREDDIT = 'subreddit'
COL_TITLE = 'title'
COL_SELFTEXT = 'selftext'
COL_CONTENTS = 'contents'
COL_WORDS = 'words'
COL_TOKENS_LEMMA = 'lemma_tokens'
COL_TOKENS_STEM = 'stem_tokens'
COL_BATCH_ID = 'batch_id'
COL_CREATED = 'created'

host_mysql = 'dsi.cu8bjyjbbev7.us-east-2.rds.amazonaws.com'
port_mysql = 3306
dbname_mysql = 'project3'
user_mysql = 'admin'
password_mysql = '12341234'


def get_connection():

    return connect(host=host_mysql, port=port_mysql, database=dbname_mysql, user=user_mysql, password=password_mysql)


def close_connection(conn):
    conn.close()


def execute_sql(conn, sql, list_values=[], close_conn=False):

    try:

        cursor = conn.cursor(prepared=True)

        if len(list_values) == 0:
            cursor.execute(sql)

        else:
            cursor.executemany(sql, list_values)

            print(cursor.rowcount, " posts inserted successfully into database")

        conn.commit()

    finally:

        rows = None

        if cursor.with_rows:
            rows = cursor.fetchall()

        if close_conn:
            conn.close()

        return rows


def setup_tables(conn):

    sql_posts_create = 'CREATE TABLE ' + TBL_POSTS_REDDIT + ' (' + \
                                         COL_NAME + ' varchar(30) COLLATE utf8_bin NOT NULL, ' + \
                                         COL_SUBREDDIT + ' varchar(255) COLLATE utf8_bin NOT NULL, ' + \
                                         COL_TITLE + ' varchar(1000) COLLATE utf8_bin, ' + \
                                         COL_SELFTEXT + ' text COLLATE utf8_bin, ' + \
                                         COL_BATCH_ID + ' int(10) unsigned NOT NULL, ' + \
                                         COL_CREATED + ' timestamp NOT NULL, ' + \
                                         'PRIMARY KEY (' + COL_NAME + ') ' + \
                                         ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin'

    execute_sql(conn, sql_posts_create, [])


def insert_reddit_posts(conn, posts):

    sql_posts_insert = 'INSERT INTO ' + TBL_POSTS_REDDIT + ' (' + \
                                        COL_NAME + ', ' + \
                                        COL_SUBREDDIT + ', ' + \
                                        COL_TITLE + ', ' + \
                                        COL_SELFTEXT + ', ' + \
                                        COL_BATCH_ID + ', ' + \
                                        COL_CREATED + ') VALUES (%s, %s, %s, %s, %s, %s)'

    list_values = [(post.name, post.subreddit, post.title, post.text, post.batch_id, post.created) for post in posts]

    execute_sql(conn, sql_posts_insert, list_values)


def get_next_reddit_batch_id(conn):

    sql_next_batch_id = 'SELECT MAX(' + COL_BATCH_ID + ') FROM ' + TBL_POSTS_REDDIT

    results = execute_sql(conn, sql_next_batch_id)

    for row in results:
        if row[0] is None:
            return 1
        else:
            return row[0] + 1


def get_reddit_post_names(conn):

    sql_post_names = 'SELECT ' + COL_NAME + ' FROM ' + TBL_POSTS_REDDIT

    return [row[0] for row in execute_sql(conn, sql_post_names)]


def get_reddit_posts_by_batch(conn, batch_id):

    sql_posts = 'SELECT ' + COL_NAME + \
                     ', ' + COL_SUBREDDIT + \
                     ', ' + COL_TITLE + \
                     ', ' + COL_SELFTEXT + \
                     ', ' + COL_BATCH_ID + \
                     ', ' + COL_CREATED + \
                 ' FROM ' + TBL_POSTS_REDDIT + \
                ' WHERE ' + COL_BATCH_ID + '=' + str(batch_id)

    return [RedditPost(row[0], row[1], row[2], row[3], created=row[5], batch_id=row[4]) for row in execute_sql(conn, sql_posts)]


def get_reddit_posts_for_nlp(conn):

    sql_posts = 'SELECT * FROM ' + TBL_POSTS_REDDIT + ' WHERE ' + COL_CONTENTS + " IS NULL"

    results = execute_sql(conn, sql_posts)

    return [RedditPost(row[0], row[1], row[2], row[3], created= row[7], batch_id=row[6]) for row in results]


def to_nlp_post(row):

    if row[3] is None:
        words = []
    else:
        words = str(row[3]).split(' ')

    if row[4] is None:
        lemma_tokens = []
    else:
        lemma_tokens = str(row[4]).split(' ')

    if row[5] is None:
        stem_tokens = []
    else:
        stem_tokens = str(row[5]).split(' ')

    post = NLPPost()

    post.name = row[0]
    post.subreddit = row[1]
    post.contents = row[2]
    post.words = row[3]
    post.lemma_tokens = row[4]
    post.stem_tokens = row[5]

    return post


def get_nlp_post(conn, name):

    sql_get = 'SELECT ' + COL_NAME + \
                   ', ' + COL_SUBREDDIT + \
                   ', ' + COL_CONTENTS + \
                   ', ' + COL_WORDS + \
                   ', ' + COL_TOKENS_LEMMA + \
                   ', ' + COL_TOKENS_STEM + \
               ' FROM ' + TBL_POSTS_REDDIT + \
              ' WHERE ' + COL_NAME + '="' + name + '"'

    results = execute_sql(conn, sql_get)

    post = None

    if results is not None:
        post = to_nlp_post(results[0])

    return post


def get_all_nlp_posts(conn):

    sql_get_all = 'SELECT ' + COL_NAME + \
                       ', ' + COL_SUBREDDIT + \
                       ', ' + COL_CONTENTS + \
                       ', ' + COL_WORDS + \
                       ', ' + COL_TOKENS_LEMMA + \
                       ', ' + COL_TOKENS_STEM + \
                   ' FROM ' + TBL_POSTS_REDDIT

    results = execute_sql(conn, sql_get_all)

    return [to_nlp_post(row) for row in results]


def update_nlp_posts(conn, posts):

    sql_posts_update = 'UPDATE ' + TBL_POSTS_REDDIT + \
                         ' SET ' + COL_CONTENTS + \
                         '=%s, ' + COL_WORDS + \
                         '=%s, ' + COL_TOKENS_LEMMA + \
                         '=%s, ' + COL_TOKENS_STEM + \
                    '=%s WHERE ' + COL_NAME + '=%s'

    list_values = [(post.contents, ' '.join(post.words), ' '.join(post.lemma_tokens), ' '.join(post.stem_tokens), post.name) for post in posts]

    execute_sql(conn, sql_posts_update, list_values)
















