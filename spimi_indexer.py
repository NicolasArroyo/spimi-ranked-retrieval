import sys
import os
import json
import heapq
import math
import nltk
import logging
from text_processing_pipeline import process_files_txt
from csv_processing_pipeline import process_files_csv

PAGE_SIZE = 4096


def parse_line(line):
    parts = line.strip().split(',')
    word = parts[0]
    postings = [(parts[i], int(parts[i + 1])) for i in range(1, len(parts), 2)]
    return word, postings


class ParsedLine:
    def __init__(self, word, postings, file_index):
        self.word = word
        self.postings = postings
        self.file_index = file_index

    def __repr__(self):
        return f"({self.word} -> {self.postings})"

    def __lt__(self, other):
        if self.word != other.word:
            return self.word < other.word

        min_length = min(len(self.postings), len(other.postings))
        for i in range(min_length):
            self_posting = int(self.postings[i][0])
            other_posting = int(other.postings[i][0])
            if self_posting < other_posting:
                return True
            elif self_posting > other_posting:
                return False

        return len(self.postings) < len(other.postings)


def merge_blocks(blocks_folder, output_file):
    block_files = [os.path.join(blocks_folder, f) for f in os.listdir(blocks_folder) if f.endswith('.txt')]
    readers = [open(file, 'r', encoding='utf-8') for file in block_files]

    pq = []
    for i, reader in enumerate(readers):
        line = reader.readline()
        if line:
            word, postings = parse_line(line)
            heapq.heappush(pq, ParsedLine(word, postings, i))

    current_writer_line = []
    with open(output_file, 'w', encoding='utf-8') as writer:
        while pq:
            min_element = heapq.heappop(pq)
            current_word = min_element.word
            file_idx = min_element.file_index
            current_postings = min_element.postings

            line = readers[file_idx].readline()
            if line:
                word, postings = parse_line(line)
                heapq.heappush(pq, ParsedLine(word, postings, file_idx))

            if len(current_writer_line) > 0 and current_writer_line[0] != current_word:
                text_to_write = ""
                for elem in current_writer_line:
                    text_to_write += f"{elem},"
                writer.write(text_to_write[:-1] + "\n")
                current_writer_line = []

            if len(current_writer_line) == 0:
                current_writer_line.append(current_word)
                for elem in current_postings:
                    current_writer_line.append(int(elem[0]))
                    current_writer_line.append(elem[1])

            else:
                if current_word == current_writer_line[0]:
                    if int(current_postings[0][0]) == current_writer_line[-2]:
                        current_writer_line[-1] += current_postings[0][1]

                        for elem in current_postings[1:]:
                            current_writer_line.append(int(elem[0]))
                            current_writer_line.append(elem[1])
                    else:
                        for elem in current_postings:
                            current_writer_line.append(int(elem[0]))
                            current_writer_line.append(elem[1])

        text_to_write = ""
        for elem in current_writer_line:
            text_to_write += f"{elem},"
        writer.write(text_to_write[:-1] + "\n")

    for reader in readers:
        reader.close()


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


def save_block(block_name, block_dict, blocks_dir):
    with open(f"{blocks_dir}/{block_name}.txt", "w") as file:
        for key in block_dict:
            file.write(f"{key}")
            for elem in block_dict[key]:
                file.write(f",{elem[0]},{elem[1]}")
            file.write("\n")


def sort_and_save_block(dictionary, block_counter, blocks_dir):
    sorted_dictionary = dict(sorted(dictionary.items()))
    save_block(f"block{block_counter}", sorted_dictionary, blocks_dir)
    # print(block_counter, get_total_size(sorted_dictionary), sorted_dictionary)

    return sorted_dictionary


def spimi_invert(token_stream, blocks_dir):
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
            sort_and_save_block(dictionary_checkpoint, block_counter, blocks_dir)

            dictionary = {token_word: [[token_file_id, 1]]}
            block_counter += 1

    if dictionary:
        sort_and_save_block(dictionary, block_counter, blocks_dir)


def calculate_tfidf(merged_index_path, filenames_path, output_path):
    with open(filenames_path, "r") as filenames_json:
        filenames_len = len(json.load(filenames_json))

    with open(merged_index_path, "r") as merged_index_file, open(output_path, "w") as output_file:
        for line in merged_index_file:
            parts = line.strip().split(',')
            word = parts[0]
            doc_freq = len(parts) // 2

            idf = math.log(filenames_len / doc_freq)
            # print(f"word: {word} | {filenames_len} / {doc_freq} = {idf}")

            new_line = [word]
            for i in range(1, len(parts), 2):
                file_id = parts[i]
                tf = int(parts[i + 1])
                tfidf = tf * idf
                new_line.extend([file_id, str(tfidf)])

            output_file.write(','.join(new_line) + '\n')


def compute_vector_score(query, merged_index_tfidf_path, k):
    porter_stemmer = nltk.PorterStemmer()

    index = {}
    with open(merged_index_tfidf_path, "r") as file:
        for line in file:
            parts = line.strip().split(',')
            word = parts[0]
            postings = {int(parts[i]): float(parts[i + 1]) for i in range(1, len(parts), 2)}
            index[word] = postings

    query_terms = query.lower().split()
    query_terms = [porter_stemmer.stem(term) for term in query_terms]
    query_weights = {}
    for term in query_terms:
        if term in index:
            query_weights[term] = math.log(1 + query_terms.count(term))

    doc_scores = {}
    for term, weight in query_weights.items():
        for doc_id, doc_weight in index.get(term, {}).items():
            if doc_id not in doc_scores:
                doc_scores[doc_id] = 0
            doc_scores[doc_id] += weight * doc_weight

    sorted_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)
    sorted_docs = sorted_docs[0:k]
    return sorted_docs


def initialize_index(blocks_dir):
    for file in ['merged_index.txt', 'filenames_dict.json']:
        file_path = os.path.join('./', file)
        if os.path.isfile(file_path):
            os.remove(file_path)

    for filename in os.listdir(blocks_dir):
        file_path = os.path.join(blocks_dir, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)

    try:
        spimi_invert(process_files_txt("./books", "./"), blocks_dir)
        merge_blocks('./blocks', 'merged_index.txt')
        calculate_tfidf("./merged_index.txt", "./filenames_dict.json", "./merged_index_tfidf.txt")

    except Exception as e:
        logging.error(f"Error while executing: {e}")


def initialize_index_csv(blocks_dir, merge_index_path, filenames_dict_path, csv_path):
    for file in ['merged_index.txt', 'filenames_dict.json']:
        file_path = os.path.join('./', file)
        if os.path.isfile(file_path):
            os.remove(file_path)

    for filename in os.listdir(blocks_dir):
        file_path = os.path.join(blocks_dir, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)

    try:
        spimi_invert(process_files_csv(csv_path, "./filenames_dict.json"), blocks_dir)
        merge_blocks('./blocks', 'merged_index.txt')
        calculate_tfidf("./merged_index.txt", "./filenames_dict.json", "./merged_index_tfidf.txt")

    except Exception as e:
        logging.error(f"Error while executing: {e}")


if __name__ == "__main__":
    initialize_index_csv("./blocks", "./merged_index.txt", "./filenames_dict.json",
                         "./songs/spotify_songs_filtered_1k.csv")
