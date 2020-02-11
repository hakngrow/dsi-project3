from bs4 import BeautifulSoup

import regex as re

from nltk.stem import WordNetLemmatizer

from nltk.stem.porter import PorterStemmer

from nltk.corpus import stopwords

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer

from sklearn.model_selection import train_test_split

from utils.aws import AWSDB


_lemmatizer = WordNetLemmatizer()
_stemmer = PorterStemmer()

_vectorizer = CountVectorizer(analyzer="word", tokenizer=None, preprocessor=None, stop_words=None, max_features=5000)


def remove_emojis(text):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               "]+", flags=re.UNICODE)

    return emoji_pattern.sub(r'', text)


def post_to_words(text):
    # Remove HTML tags
    text = BeautifulSoup(text, features='html5lib').get_text()

    # Remove digits and special characters
    letters_only = re.sub("[^a-zA-Z]", " ", text)

    # Convert to lower case and split into individual words
    words = letters_only.lower().split()

    # Get stop words
    stops = set(stopwords.words('english'))

    # Remove stop words
    meaningful_words = [word for word in words if not word in stops]

    return meaningful_words


def lemmatize_words(words):

    return [_lemmatizer.lemmatize(word) for word in words]


def stem_words(words):

    return [_stemmer.stem(word) for word in words]


def get_count_vectorizer():

    return _vectorizer


def count_vectorize(tokens_train, tokens_test, features_count):

    _vectorizer.max_features(features_count)

    features_train = _vectorizer.fit_transform(tokens_train)
    features_test = _vectorizer.fit(tokens_test)

    return features_train, features_test


def train_test_split_tokens(posts, test_size, random_state):

    all_tokens = []
    all_targets = []

    for post in posts:
        all_tokens.append(post.tokens)
        all_targets.append(post.subreddit)

    return train_test_split(all_tokens, all_targets, test_size=test_size, random_state=random_state)


