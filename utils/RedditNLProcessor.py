from utils.aws import AWSDB

from utils import NLPUtils

from utils.model.post import NLPPost

from pprint import pprint


# Retrieve Reddit posts for pre-processing (cleaning, tokenizing, lemmatization, stemming)
# Limit thw number of posts to process via the posts_limit parameter
def process_posts(lemmatize=False, stem=False, posts_limit=0):

    # Retrieve Reddit posts from AWS RDS that have not been pre-processed
    reddit_posts = AWSDB.get_reddit_posts_for_nlp(posts_limit)

    # Perform cleaning and tokenizing by instantiating NLPPost objects from Reddit posts
    nlp_posts = [NLPPost(reddit_post=post) for post in reddit_posts]

    # Check whether to lemmatize posts to tokens
    if lemmatize:
        for post in nlp_posts:
            post.lemma_tokens = NLPUtils.lemmatize_words(post.words)

    # Check whether to stem posts to tokens
    if stem:
        for post in nlp_posts:
            post.stem_tokens = NLPUtils.stem_words(post.words)

    # Retrieve the next batch id
    next_batch_id = AWSDB.get_next_reddit_batch_id()

    # Update processed NLP post with batch id
    for post in nlp_posts:
        post.batch_id = next_batch_id

    # Persist processed NLP posts back to AWS RDS
    AWSDB.insert_nlp_posts(nlp_posts)

    # Get all names of the processed reddit posts
    names = [(post.name,) for post in nlp_posts]

    # Update batch id of reddit posts processed
    AWSDB.update_reddit_posts_batch_id(names, next_batch_id)


process_posts(lemmatize=True, stem=True)




