from mysql.connector import connect
from mysql.connector import Error

from utils.model.post import RedditPost, NLPPost

TBL_POSTS_REDDIT = 'posts'
TBL_POSTS_NLP = 'posts_nlp'

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


# Return a connection to the MySQL database using the above defined connection parameters
def get_connection():
    return connect(host=host_mysql, port=port_mysql, database=dbname_mysql, user=user_mysql, password=password_mysql)


# Close connection to the MySQL database
def close_connection(conn):
    conn.close()


# Execute a SQL prepared statement with the list_values as an array of parameters to the statement
def execute_sql(conn, sql, list_values=[], close_conn=False):

    rows = None

    try:

        cursor = conn.cursor(prepared=True)

        if len(list_values) == 0:

            cursor.execute(sql)

            if cursor.with_rows:
                rows = cursor.fetchall()

        else:

            cursor.executemany(sql, list_values)

            print(cursor.rowcount, ' posts inserted successfully into database')

        conn.commit()

    except Error as error:

        print(f'sql query failed: {error}')

    finally:

        if close_conn:
            conn.close()

        return rows


# Create database tables required
def setup_tables(conn):

    sql_reddit_create = 'CREATE TABLE ' + TBL_POSTS_REDDIT + ' (' + \
                        COL_NAME + ' varchar(30) COLLATE utf8_bin NOT NULL, ' + \
                        COL_SUBREDDIT + ' varchar(255) COLLATE utf8_bin NOT NULL, ' + \
                        COL_TITLE + ' varchar(1000) COLLATE utf8_bin, ' + \
                        COL_SELFTEXT + ' text COLLATE utf8_bin, ' + \
                        COL_BATCH_ID + ' int(10) unsigned NOT NULL, ' + \
                        COL_CREATED + ' timestamp NOT NULL, PRIMARY KEY (' + \
                        COL_NAME + ')) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin'

    sql_posts_create = 'CREATE TABLE ' + TBL_POSTS_REDDIT + ' (' + \
                       COL_NAME + ' varchar(30) COLLATE utf8_bin NOT NULL, ' + \
                       COL_SUBREDDIT + ' varchar(255) COLLATE utf8_bin NOT NULL, ' + \
                       COL_CONTENTS + ' text COLLATE utf8_bin, ' + \
                       COL_WORDS + ' text COLLATE utf8_bin, ' + \
                       COL_TOKENS_LEMMA + ' text COLLATE utf8_bin, ' + \
                       COL_TOKENS_STEM + ' text COLLATE utf8_bin, ' + \
                       COL_CREATED + ' timestamp NOT NULL, PRIMARY KEY (' + \
                       COL_NAME + ')) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin'

    execute_sql(conn, sql_posts_create, [])


# Create an array of Reddit posts
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


# Returns the next batch Id of Reddit post to process
def get_next_reddit_batch_id(conn):

    sql_next_batch_id = 'SELECT MAX(' + COL_BATCH_ID + ') FROM ' + TBL_POSTS_REDDIT

    results = execute_sql(conn, sql_next_batch_id)

    for row in results:
        if row[0] is None:
            return 1
        else:
            return row[0] + 1


# Returns all the Reddit posts by subreddit
def get_reddit_posts(conn, subreddit):

    sql_posts = f'SELECT * FROM ' + TBL_POSTS_REDDIT + \
                        ' WHERE ' + COL_SUBREDDIT + '=\'' + subreddit + '\''

    return [RedditPost(row[0], row[1], row[2], row[3], created=row[5], batch_id=row[4])
            for row in execute_sql(conn, sql_posts)]


# Returns all the Reddit post names
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

    return [RedditPost(row[0], row[1], row[2], row[3], created=row[5], batch_id=row[4]) for row in
            execute_sql(conn, sql_posts)]


def get_reddit_posts_for_nlp(conn, limit=0):

    sql_posts = 'SELECT * FROM ' + TBL_POSTS_REDDIT + ' WHERE ' + COL_BATCH_ID + "=0"

    if limit > 0:
        sql_posts = sql_posts + ' LIMIT ' + str(limit)

    results = execute_sql(conn, sql_posts)

    return [RedditPost(row[0], row[1], row[2], row[3], created=row[4], batch_id=row[5]) for row in results]


def update_reddit_posts_batch_id(conn, names, batch_id):

    sql_update_batch_id = 'UPDATE ' + TBL_POSTS_REDDIT + \
                            ' SET ' + COL_BATCH_ID + '=' + str(batch_id) + \
                          ' WHERE ' + COL_NAME + '=%s'

    execute_sql(conn, sql_update_batch_id, names)


def to_nlp_post(row):

    if row[4] is None:
        words = []
    else:
        words = str(row[4]).split(' ')

    if row[5] is None:
        lemma_tokens = []
    else:
        lemma_tokens = str(row[5]).split(' ')

    if row[6] is None:
        stem_tokens = []
    else:
        stem_tokens = str(row[6]).split(' ')

    return NLPPost(name=row[0], subreddit=row[1], batch_id=row[2], contents=row[3], words=words,
                   lemma_tokens=lemma_tokens, stem_tokens=stem_tokens)


def get_nlp_post(conn, name):

    sql_get = 'SELECT * FROM ' + TBL_POSTS_NLP + \
                     ' WHERE ' + COL_NAME + '="' + name + '"'

    results = execute_sql(conn, sql_get)

    post = None

    if results is not None:
        post = to_nlp_post(results[0])

    return post


def get_all_nlp_posts(conn):

    sql_get_all = 'SELECT * FROM ' + TBL_POSTS_NLP

    results = execute_sql(conn, sql_get_all)

    return [to_nlp_post(row) for row in results]


# Insert newly pre-processed posts for NLP
def insert_nlp_posts(conn, posts):

    sql_posts_insert = 'INSERT INTO ' + TBL_POSTS_NLP + \
                                 ' (' + COL_NAME + \
                                 ', ' + COL_SUBREDDIT + \
                                 ', ' + COL_BATCH_ID + \
                                 ', ' + COL_CONTENTS + \
                                 ', ' + COL_WORDS + \
                                 ', ' + COL_TOKENS_LEMMA + \
                                 ', ' + COL_TOKENS_STEM + ') VALUES (%s, %s, %s, %s, %s, %s, %s)'

    list_values = [(post.name, post.subreddit, post.batch_id, post.contents,
                    ' '.join(post.words), ' '.join(post.lemma_tokens), ' '.join(post.stem_tokens)) for post in posts]

    execute_sql(conn, sql_posts_insert, list_values)


def get_next_nlp_batch_id(conn):

    sql_next_batch_id = 'SELECT MAX(' + COL_BATCH_ID + ') FROM ' + TBL_POSTS_NLP

    results = execute_sql(conn, sql_next_batch_id)

    for row in results:
        if row[0] is None:
            return 1
        else:
            return row[0] + 1
