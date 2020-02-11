import pprint as pp
import json

import utils.aws.AWSDB as db


posts = db.get_reddit_posts('worldnews')

for post in posts:
    pp.pprint(post.__dict__, indent=4)


