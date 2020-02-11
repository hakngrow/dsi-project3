import datetime

from utils import NLPUtils


# Parent class that models a generic forum post
class Post:

    def __init__(self, name, title):

        self.name = name        # Unique Id of the post
        self.title = title      # Title of the post


# Class that models a Reddit post
class RedditPost(Post):

    def __init__(self, name, subreddit, title, text, created_utc=None, created=None, post_dict=None, batch_id=0):

        # RedditPost can be initialized by passing in a JSON dictionary from the Reddit API
        if post_dict is not None:

            name = post_dict['name']
            subreddit = post_dict['subreddit']
            title = post_dict['title']
            text = post_dict['selftext']
            batch_id = 0
            created = int(post_dict['created_utc'])

        self.name = name                    # Inherits from the parent class Post
        self.subreddit = subreddit          # Sub-Reddit of the post
        self.title = title                  # Inherits from the parent class Post
        self.text = text                    # Textual contents of the post
        self.created_utc = created_utc      # UTC creation timestamp
        self.created = created              # Timestamp equivalent created_utc
        self.batch_id = batch_id            # Id to identify the batch of posts that was pre-processed

        # Convert UTC to timestamp data type
        if self.created_utc is not None:
            self.created = datetime.datetime.utcfromtimestamp(self.created_utc)


# Class that models the a Reddit post after pre-processing for NLP
class NLPPost(RedditPost):

    # Initialize NLPPost with a RedditPost
    def init_from_reddit_post(self, reddit_post):

        self.name = reddit_post.name
        self.subreddit = reddit_post.subreddit
        self.batch_id = reddit_post.batch_id
        self.contents = reddit_post.title + ' ' + reddit_post.text
        self.words = NLPUtils.post_to_words(self.contents)
        self.lemma_tokens = []
        self.stem_tokens = []

    def __init__(self, name=None, subreddit=None, batch_id=0, contents=None, words=None, lemma_tokens=None,
                 stem_tokens=None, reddit_post=None):

        self.name = name                        # Inherits from the parent class
        self.subreddit = subreddit              # Inherits from the parent class
        self.batch_id = batch_id                # Inherits from the parent class
        self.contents = contents                # Combines the Reddit post's title and textual contents
        self.words = words                      # Meaning words after pre-processing
        self.lemma_tokens = lemma_tokens        # Tokens derived from lemmatization
        self.stem_tokens = stem_tokens          # Tokens derived from stemming

        if reddit_post is not None:
            self.init_from_reddit_post(reddit_post)
