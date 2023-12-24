import json
import os
import string
import nltk
import csv

if not nltk.data.find('tokenizers/punkt'):
    nltk.download('punkt')

tweet_tokenizer = nltk.TweetTokenizer()
porter_stemmer = nltk.PorterStemmer()


def tokenize_line(line):
    # Design: Use TweetTokenizer so it doesn't separate contractions
    # return tweet_tokenizer.tokenize(line)

    # Otherwise, comment previous line and uncomment the next one
    return nltk.word_tokenize(line)


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


def create_filenames_json(output_dir, filenames):
    filenames_dict = {}

    for i, filename in enumerate(filenames):
        new_key = f"doc{i}"
        filenames_dict[new_key] = filename

    with open(output_dir + "/filenames.json", "w", encoding="utf-8") as file:
        json.dump(filenames_dict, file, indent=4)


def process_csv_file(csv_path, filenames_json_output_dir):
    files_ids = []

    print("DEBUG: Started yielding tokens")

    with open(csv_path, "r") as file:
        csv_reader = csv.reader(file)
        next(csv_reader)

        for i, row in enumerate(csv_reader):
            if i % 100 == 0:
                print(f"DEBUG: Yielding {i}th row")

            if row:
                new_file = {
                    "id": row[0],
                    "name": row[1],
                    "artist": row[2],
                    "album_name": row[4]
                }

                # file_id = row[0]
                files_ids.append(new_file)

                tokens = tokenize_line(','.join(row))
                tokens = normalize_tokens(tokens)
                tokens = stem_tokens(tokens)

                for word in tokens:
                    if word:
                        yield [word, len(files_ids) - 1]

    print("DEBUG: Finished yielding tokens")

    create_filenames_json(filenames_json_output_dir, files_ids)
    print("DEBUG: Filenames json saved")


def process_text_files(texts_dir, filenames_json_output_dir):
    files_ids = []

    print("DEBUG: Started yielding tokens")

    for filename in os.listdir(texts_dir):
        files_ids.append(filename)
        file_path = os.path.join(texts_dir, filename)

        with open(file_path, "r", encoding="utf-8") as file:
            for line in file:
                tokens = tokenize_line(line)
                tokens = normalize_tokens(tokens)
                tokens = stem_tokens(tokens)

                for word in tokens:
                    if word:
                        yield [word, len(files_ids) - 1]

    print("DEBUG: Finished yielding tokens")

    create_filenames_json(filenames_json_output_dir, files_ids)
    print("DEBUG: Filenames json saved")


if __name__ == "__main__":
    for elem in process_csv_file("./songs/spotify_songs_filtered_1k.csv", "./"):
        pass

    for elem in process_text_files("./books", "./"):
        pass
