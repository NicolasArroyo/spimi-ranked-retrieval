import sys
import os
import json
from text_processing_pipeline import process_files_txt

PAGE_SIZE = 4096


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


def sort_and_save_block(dictionary, block_counter):
    sorted_dictionary = dict(sorted(dictionary.items()))
    save_block(f"block{block_counter}", sorted_dictionary)
    print(block_counter, get_total_size(sorted_dictionary), sorted_dictionary)

    return sorted_dictionary


def spimi_invert(token_stream):
    block_counter = 0
    dictionary = {}

    for token in token_stream:
        dictionary_checkpoint = dictionary.copy()

        token_word = token[0]
        token_file_id = token[1]

        # If token is not in dictionary, add its file ID with a term frequency of one
        if token_word not in dictionary:
            dictionary[token_word] = [[token_file_id, 1]]
        # Else if it already exists an occurrence of the token in the same file, add one to its term frequency
        elif dictionary[token_word][-1][0] == token_file_id:
            dictionary[token_word][-1][1] += 1
        # Else, appends its file ID with a term frequency of one
        else:
            dictionary[token_word].append([token_file_id, 1])

        if get_total_size(dictionary) > PAGE_SIZE:
            sort_and_save_block(dictionary_checkpoint, block_counter)

            dictionary = {token_word: [[token_file_id, 1]]}
            block_counter += 1

    if dictionary:
        sort_and_save_block(dictionary, block_counter)


if __name__ == "__main__":
    spimi_invert(process_files_txt("./books", "./"))

    with open("./filenames_dict.json", "r", encoding='utf-8') as file:
        filenames_dict = json.load(file)

    print(filenames_dict)

    os.remove("filenames_dict.json")
