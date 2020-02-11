import pprint as pp
import json

import utils.aws.AWSDB as db

def lambda_handler(event, context):

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


'''
posts = db.get_reddit_posts('worldnews')

for post in posts:
    pp.pprint(post.__dict__, indent=4)
'''

