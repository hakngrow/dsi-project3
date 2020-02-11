import pandas as pd

import json

from pprint import pprint

from flask import Flask, jsonify, request

from utils.aws import AWSDB

from utils import NLPUtils

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello, World!'


@app.route('/posts')
def get_post_by_name():

    name = request.args.get("name")

    post = AWSDB.get_nlp_post(name)

    if post is None:
        return f'Post {name} not found!'
    else:
        return vars(post)


@app.route('/nlp')
def nlp():

    posts = AWSDB.get_all_nlp_posts()

    print(f'Retrieved {len(posts)} posts for NLP')

    NLPUtils.do_nlp(posts, {'StarWars': 1, 'startrek': 0})



nlp()
