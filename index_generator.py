import os
import heapq
import json
import math

from dotenv import load_dotenv
from index_generator_helper import *
from token_processing_pipeline import process_csv_file

load_dotenv()


def spimi_invert(token_stream, blocks_dir):
    print("DEBUG: Started SPIMI invertion")
    block_counter = 0
    dictionary = {}

    for token in token_stream:
        dictionary_checkpoint = dictionary.copy()

        token_word = token[0]
        token_file_id = token[1]

        if token_word in dictionary:
            if dictionary[token_word][-1][0] == token_file_id:
                dictionary[token_word][-1][1] += 1
            else:
                dictionary[token_word].append([token_file_id, 1])
        else:
            dictionary[token_word] = [[token_file_id, 1]]

        if get_total_size(dictionary) > int(os.getenv("PAGE_SIZE")):
            sort_and_save_block(dictionary_checkpoint, block_counter, blocks_dir)

            dictionary = {token_word: [[token_file_id, 1]]}
            block_counter += 1

    if dictionary:
        sort_and_save_block(dictionary, block_counter, blocks_dir)

    print("DEBUG: Finished SPIMI invertion")


def create_merged_index(blocks_dir, merge_index_output_path):
    print("DEBUG: Started creation of merged index")

    block_files = [os.path.join(blocks_dir, f) for f in os.listdir(blocks_dir) if f.endswith('.txt')]
    readers = [open(file, 'r', encoding='utf-8') for file in block_files]

    pq = []
    for i, reader in enumerate(readers):
        line = reader.readline()
        if line:
            word, postings = parse_line(line)
            heapq.heappush(pq, ParsedLine(word, postings, i))

    current_writer_line = []
    with open(merge_index_output_path, 'w', encoding='utf-8') as writer:
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

    print("DEBUG: Finished creation of merged index")


def create_merged_weighted_index(merged_index_path, filenames_json_path, output_path):
    print("DEBUG: Started creation of merged weighted index")

    with open(filenames_json_path, "r") as filenames_json:
        filenames_len = len(json.load(filenames_json))

    with open(merged_index_path, "r") as merged_index_file, open(output_path, "w") as output_file:
        for line in merged_index_file:
            parts = line.strip().split(',')
            word = parts[0]
            doc_freq = len(parts) // 2

            idf = math.log(filenames_len / doc_freq)

            new_line = [word]
            for i in range(1, len(parts), 2):
                file_id = parts[i]
                tf = int(parts[i + 1])
                tfidf = tf * idf
                new_line.extend([file_id, str(tfidf)])

            output_file.write(','.join(new_line) + '\n')

    print("DEBUG: Finished creation of merged weighted index")


def generate_index():
    output_dir = "./index_output"
    blocks_dir = "./index_output/blocks"

    clear_folder(output_dir)

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(blocks_dir, exist_ok=True)

    token_stream = process_csv_file("./songs/songs_filter_16k.csv", output_dir)
    spimi_invert(token_stream, blocks_dir)

    create_merged_index(blocks_dir, f"{output_dir}/merged_index.txt")
    create_merged_weighted_index(f"{output_dir}/merged_index.txt",
                                 f"{output_dir}/filenames.json",
                                 f"{output_dir}/merged_weighted_index.txt")


if __name__ == "__main__":
    # clear_folder("./test-blocks")

    # spimi_invert(process_csv_file("./songs_filter_100.csv",
    #                               "./"),
    #              "./test-blocks")

    # output_dir = "./index_output"
    # blocks_dir = "./index_output/blocks"
    #
    # create_merged_weighted_index(f"{output_dir}/merged_index.txt",
    #                              f"{output_dir}/filenames.json",
    #                              f"{output_dir}/merged_weighted_index.txt")
    #
    # create_merged_index("./index_output/blocks", "./index_output/merged_index.txt")
    generate_index()
