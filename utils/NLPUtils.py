import pandas as pd

from bs4 import BeautifulSoup

import regex as re

from nltk.stem import WordNetLemmatizer

from nltk.stem.porter import PorterStemmer

from nltk.corpus import stopwords

from sklearn.feature_extraction import stop_words
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer

from sklearn.linear_model import LogisticRegression

from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score

from sklearn.pipeline import Pipeline

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


def post_to_words(text, my_stop_words=None):

    # Remove HTML tags
    text = BeautifulSoup(text, features='html5lib').get_text()

    # Remove digits and special characters
    letters_only = re.sub("[^a-zA-Z]", " ", text)

    # Convert to lower case and split into individual words
    words = letters_only.lower().split()

    # Get NLTK stop words
    stops = set(stopwords.words('english'))

    # Add new stop words using the my_stop_words parameter
    if my_stop_words is not None:
        stops.update(my_stop_words)

    # Remove stop words
    meaningful_words = [word for word in words if not word in stops]

    return meaningful_words


def lemmatize_words(words):

    return [_lemmatizer.lemmatize(word) for word in words]


def stem_words(words):

    return [_stemmer.stem(word) for word in words]


def get_count_vectorizer():

    return _vectorizer


def posts_to_dataframe(posts, map_target, use_lemmatized_tokens=True):

    rows = [[post.lemma_tokens, post.subreddit] if use_lemmatized_tokens else [post.stem_tokens, post.subreddit] for post in posts]

    df = pd.DataFrame(rows, columns=['tokens', 'subreddit'])

    df['target'] = df['subreddit'].map(map_target)
    df['contents'] = df['tokens'].apply(lambda tokens: ' '.join(tokens))

    return df


def get_nlp_pipeline():

    cvec = CountVectorizer()
    lr = LogisticRegression()

    pipe = Pipeline([("cvec", cvec), ("lr", lr)])

    print(pipe.steps)

    pipe_params = {
        'cvec__max_features': [2500, 3000, 3500],
        'cvec__min_df': [2, 3],
        'cvec__max_df': [.9, .95],
        'cvec__ngram_range': [(1, 1), (1, 2)]
    }

    return GridSearchCV(pipe, param_grid=pipe_params, cv=3, n_jobs=-1)


def do_nlp(posts, map_target):

    df_posts = posts_to_dataframe(posts, map_target)

    #df_posts.to_csv('posts.csv')

    baseline = df_posts['subreddit'].value_counts(normalize=True)

    print(baseline)

    print(df_posts.head())

    X = df_posts['contents']
    y = df_posts['target']

    X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y, random_state=19)

    pipe = Pipeline([
        ('cvec', CountVectorizer()),
        ('lr', LogisticRegression())
    ])

    # Evaluate how your model will perform on unseen data
    cross_val_score(pipe, X_train, y_train, cv=3)

    # Fit your model
    pipe.fit(X_train, y_train)

    # Training score
    score = pipe.score(X_train, y_train)

    print(score)

    # Test score
    pipe.score(X_test, y_test)

    print(score)

    pipe_params = {
        'cvec__max_features': [2500, 3000, 3500],
        'cvec__min_df': [2, 3],
        'cvec__max_df': [.9, .95],
        'cvec__ngram_range': [(1, 1), (1, 2)]
    }
    gs = GridSearchCV(pipe, param_grid=pipe_params, cv=3)
    gs.fit(X_train, y_train)
    print(gs.best_score_)
    gs.best_params_

    print(gs.score(X_train, y_train))

    print(gs.score(X_test, y_test))
