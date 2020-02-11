import praw

from pprint import pprint

from utils.model.post import RedditPost

from utils.aws import AWSDB


# Establish a connection to Reddit via the PRAW wrapper api
reddit = praw.Reddit(client_id='decGUQHbZSwTxA', client_secret='ePMxrPWEiouDDV07dZiVpJ08y4g', user_agent='project3')


# Scrap posts by sub reddit and limit the number of posts retrieved
def scrap_posts(subreddit, limit):

    # Create an instance of the sub reddit
    sr = reddit.subreddit(subreddit)

    print(f'Scrapping {limit} posts from subreddit ' + subreddit)

    # Retrieve most recent posts specified by argument limit
    posts = sr.new(limit=limit)

    # Get existing post names to check for duplicate posts
    existing_posts = AWSDB.get_reddit_post_names()
    current_posts = {}

    scrap_count = 0

    for post in posts:

        scrap_count += 1

        # pprint(vars(post))

        # Check that post is distinct
        if (post.name in current_posts.keys()) or (post.name in existing_posts):

            print('Duplicate post found: ' + post.name)

        else:

            # Create reddit post
            rd_post = RedditPost(post.name, post.subreddit_name_prefixed[2:], post.title, post.selftext,
                                 post.created_utc, batch_id=0)

            current_posts[post.name] = rd_post

            print(f'Scrapping post #{scrap_count}: {post.name}')

    print(f'Scrapped {scrap_count} posts, unique posts = {len(current_posts)}')

    # Save scrapped posts in AWS RDS Mysql
    AWSDB.insert_reddit_posts(list(current_posts.values()))

scrap_posts('worldnews', 300)

