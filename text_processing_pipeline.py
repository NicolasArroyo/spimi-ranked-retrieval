import os
import json
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
    for i, token in enumerate(tokens):
        # Design: Delete tokens that contains punctuation
        if token in string.punctuation:
            continue

        # Design: Delete tokens that can be cast to a number
        # if token.isnumeric():
        #     continue

        # Design: Delete tokens that contain numbers
        if any(char.isnumeric() for char in token):
            continue

        # Design: Delete tokens without alphanumeric characters
        if not any(char.isalnum() for char in token):
            continue

        # Design: Apply lowercase to the first token in each line
        yield token.lower() if i == 0 else token


def stem_tokens(tokens):
    for token in tokens:
        yield porter_stemmer.stem(token)


def update_filenames_dict(filenames_dict_dir, filename):
    filenames_dict_path = os.path.join(filenames_dict_dir, "filenames_dict.json")

    if os.path.exists(filenames_dict_path):
        with open(filenames_dict_path, "r", encoding="utf-8") as file:
            filenames_dict = json.load(file)
    else:
        filenames_dict = {}

    new_key = f"doc{len(filenames_dict)}"
    filenames_dict[new_key] = filename

    with open(filenames_dict_path, "w", encoding="utf-8") as file:
        json.dump(filenames_dict, file, indent=4)

    return len(filenames_dict) - 1


def process_files_txt(files_dir, filenames_dict_dir):
    for filename in os.listdir(files_dir):
        file_id = update_filenames_dict(filenames_dict_dir, filename)
        filepath = os.path.join(files_dir, filename)

        try:
            with open(filepath, "r", encoding="utf-8") as file:
                for line in file:
                    tokens = tokenize_line(line)
                    tokens = normalize_tokens(tokens)
                    tokens = stem_tokens(tokens)

                    for word in tokens:
                        if word:
                            yield [word, file_id]

        except IOError:
            print(f"File {filepath} could not be read.")


if __name__ == "__main__":
    for word in process_files_txt("./books/", "./"):
        print(word, end=" ")

    with open("./filenames_dict.json", "r", encoding='utf-8') as file:
        filenames_dict = json.load(file)

    print("")
    print(filenames_dict)

    os.remove("filenames_dict.json")
