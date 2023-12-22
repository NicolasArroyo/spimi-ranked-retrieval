import sys
import os
import json
from text_processing_pipeline import process_files_txt

BLOCK_SIZE = 40000


# Get total size of dictionary recursively as sys.getsizeof(dict) might give incorrect results
def get_total_size(obj, seen=None):
    size = sys.getsizeof(obj)

    if seen is None:
        seen = set()

    obj_id = id(obj)

    if obj_id in seen:
        return 0

    seen.add(obj_id)

    if isinstance(obj, dict):
        size += sum([get_total_size(v, seen) for v in obj.values()])
        size += sum([get_total_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_total_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_total_size(i, seen) for i in obj])

    return size


def save_block(block_name, block_dict):
    pass


def spimi_invert(token_stream):
    block_counter = 0
    dictionary = {}
    dictionary_checkpoint = {}

    for token in token_stream:
        dictionary_checkpoint = dictionary.copy()

        token_word = token[0]
        token_file_id = token[1]

        if token_word not in dictionary:
            dictionary[token_word] = [[token_file_id, 1]]
        elif dictionary[token_word][-1][0] == token_file_id:
            dictionary[token_word][-1][1] += 1
        elif dictionary[token_word][-1][0] != token_file_id:
            dictionary[token_word].append([token_file_id, 1])

        if get_total_size(dictionary) > BLOCK_SIZE:
            sorted_dictionary = dict(sorted(dictionary_checkpoint.items()))
            save_block(f"block{block_counter}", sorted_dictionary)

            print(block_counter, get_total_size(sorted_dictionary), sorted_dictionary)

            dictionary = {token_word: [[token_file_id, 1]]}
            dictionary_checkpoint = {}
            block_counter += 1

    if dictionary:
        sorted_dictionary = dict(sorted(dictionary.items()))
        save_block(f"block{block_counter}", sorted_dictionary)
        print(block_counter, get_total_size(sorted_dictionary), sorted_dictionary)


if __name__ == "__main__":
    spimi_invert(process_files_txt("./books", "./"))

    with open("./filenames_dict.json", "r", encoding='utf-8') as file:
        filenames_dict = json.load(file)

    print(filenames_dict)

    os.remove("filenames_dict.json")
