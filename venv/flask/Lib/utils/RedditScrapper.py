import praw

from pprint import pprint

from utils.model.post import RedditPost

from utils.aws import AWSDB

reddit = praw.Reddit(client_id='decGUQHbZSwTxA', client_secret='ePMxrPWEiouDDV07dZiVpJ08y4g', user_agent='project3')


def scrap_posts(subreddit, limit):

    sr = reddit.subreddit(subreddit)

    print(f'Scrapping {limit} posts from subreddit ' + subreddit)

    posts = sr.new(limit=limit)

    next_batch_id = AWSDB.get_next_reddit_batch_id()

    existing_posts = AWSDB.get_reddit_post_names()
    current_posts = {}

    scrap_count = 0

    for post in posts:

        scrap_count += 1

        # pprint(vars(post))

        if (post.name in current_posts.keys()) or (post.name in existing_posts):
            print('Duplicate post found: ' + post.name)

        else:
            rd_post = RedditPost(post.name, post.subreddit_name_prefixed[2:], post.title, post.selftext,
                                 post.created_utc, batch_id=next_batch_id)

            current_posts[post.name] = rd_post

            print(f'Scrapping post #{scrap_count}: {post.name}')

    print(f'Scrapped {scrap_count} posts, unique posts = {len(current_posts)}')

    AWSDB.insert_reddit_posts(list(current_posts.values()))

scrap_posts('starwars', 300)

