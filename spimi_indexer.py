import sys
import os
import json
from text_processing_pipeline import process_files_txt

BLOCK_SIZE = 4096


def save_block(block_name, block_dict):
    pass


def spimi_invert(token_stream):
    block_counter = 0
    dictionary = {}

    for token in token_stream:
        dictionary_checkpoint = dictionary

        token_word = token[0]
        token_file_id = token[1]

        if token_word not in dictionary:
            dictionary[token_word] = [token_file_id]

        elif dictionary[token_word][-1] != token_file_id:
            dictionary[token_word].append(token_file_id)

        # TODO: When returning to checkpoint one word is lost

        if sys.getsizeof(dictionary) > BLOCK_SIZE:
            dictionary = dict(sorted(dictionary.items()))
            print(dictionary)

            dictionary = dictionary_checkpoint
            save_block(f"block{block_counter}", dictionary)

            dictionary = {}
            dictionary_checkpoint = {}
            block_counter += 1


if __name__ == "__main__":
    spimi_invert(process_files_txt("./books", "./"))

    with open("./filenames_dict.json", "r", encoding='utf-8') as file:
        filenames_dict = json.load(file)

    print(filenames_dict)

    os.remove("filenames_dict.json")