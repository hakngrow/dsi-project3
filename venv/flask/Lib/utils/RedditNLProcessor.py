from utils.model.post import NLPPost

from utils.aws import AWSDB

from utils import NLPUtils


def process_posts(lemmatize=False, stem=False):

    reddit_posts = AWSDB.get_reddit_posts_for_nlp()
    nlp_posts = [NLPPost(post) for post in reddit_posts]

    if lemmatize:
        for post in nlp_posts:
            post.lemma_tokens = NLPUtils.lemmatize_words(post.words)

    if stem:
        for post in nlp_posts:
            post.stem_tokens = NLPUtils.stem_words(post.words)

    AWSDB.update_nlp_posts(nlp_posts)





process_posts(lemmatize=True, stem=True)
