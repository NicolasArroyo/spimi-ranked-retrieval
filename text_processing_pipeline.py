import os
import string

import nltk

if not nltk.data.find('tokenizers/punkt'):
    nltk.download('punkt')

tweet_tokenizer = nltk.TweetTokenizer()
porter_stemmer = nltk.PorterStemmer()


def tokenize_line(line):
    # Design: Use TweetTokenizer so it doesn't separate contractions
    return tweet_tokenizer.tokenize(line)

    # Otherwise, comment previous line and uncomment the next one
    # return nltk.word_tokenize(line)


def normalize_tokens(tokens):
    # Design: Delete tokens that contains punctuation
    tokens = [token for token in tokens if token not in string.punctuation]

    # Design: Delete tokens that can be cast to a number
    tokens = [token for token in tokens if not token.isnumeric()]

    # Design: Apply lowercase to the first token in each line
    if len(tokens) > 0:
        tokens[0] = tokens[0].lower()

    # Design: Delete tokens without alphanumeric characters
    tokens = [token for token in tokens if any(char.isalnum() for char in token)]

    return tokens


def stem_tokens(tokens):
    stemmer = nltk.PorterStemmer()
    tokens = [stemmer.stem(token) for token in tokens]

    return tokens


def process_files(directory: str):
    for filename in os.listdir(directory):
        with open(directory + filename, "r", encoding="utf-8") as file:
            for line in file:
                line = tokenize_line(line)
                line = normalize_tokens(line)
                line = stem_tokens(line)

                if len(line) == 0:
                    continue

                for word in line:
                    yield word


if __name__ == "__main__":
    for word in process_files("./books/"):
        print(word)
