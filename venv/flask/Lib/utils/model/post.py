import datetime

from utils import NLPUtils


class Post:

    def __init__(self, name, title):
        self.name = name
        self.title = title


class RedditPost(Post):

    def __init__(self, name, subreddit, title, text, created_utc=None, created=None, post_dict=None, batch_id=0):

        if post_dict is not None:

            name = post_dict['name']
            subreddit = post_dict['subreddit']
            title = post_dict['title']
            text = post_dict['selftext']
            batch_id = 0
            created = int(post_dict['created_utc'])

        Post.__init__(self, name, title)

        self.subreddit = subreddit
        self.text = text
        self.created_utc = created_utc
        self.created = created
        self.batch_id = batch_id

        if self.created_utc is not None:
            self.created = datetime.datetime.utcfromtimestamp(self.created_utc)


class NLPPost(RedditPost):

    def __init__(self, name, subreddit, contents, words, lemma_tokens, stem_tokens):

        RedditPost.__init_(self, name, subreddit, None, None)

        self.contents = contents
        self.words = words
        self.lemma_tokens = lemma_tokens
        self.stem_tokens = stem_tokens

    def __init__(self, post=None):

        RedditPost.__init__(self, post.name, post.subreddit, post.title, post.text, created=post.created, batch_id=0)

        self.contents = post.title + ' ' + post.text
        self.words = NLPUtils.post_to_words(self.contents)
        self.lemma_tokens = []
        self.stem_tokens = []

    def __init__(self):
        pass
