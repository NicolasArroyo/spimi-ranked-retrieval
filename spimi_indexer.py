import sys
import os
import json
import heapq
from text_processing_pipeline import process_files_txt

PAGE_SIZE = 40960


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


def save_block(block_name, block_dict):
    with open(f"./blocks/{block_name}.txt", "w") as file:
        for key in block_dict:
            file.write(f"{key}")
            for elem in block_dict[key]:
                file.write(f",{elem[0]},{elem[1]}")
            file.write("\n")


def sort_and_save_block(dictionary, block_counter):
    sorted_dictionary = dict(sorted(dictionary.items()))
    save_block(f"block{block_counter}", sorted_dictionary)
    # print(block_counter, get_total_size(sorted_dictionary), sorted_dictionary)

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
    for filename in os.listdir('./blocks'):
        if os.path.isfile(os.path.join('./blocks', filename)):
            os.remove(os.path.join('./blocks', filename))

    for filename in os.listdir('./'):
        if os.path.isfile(os.path.join('./', "merged_index.txt")):
            os.remove(os.path.join('./', "merged_index.txt"))

    try:
        spimi_invert(process_files_txt("./books", "./"))
        merge_blocks('./blocks', 'merged_index.txt')

    except:
        print("Error while executing")

    with open("./filenames_dict.json", "r", encoding='utf-8') as file:
        filenames_dict = json.load(file)

    print(json.dumps(filenames_dict, indent=1))

    os.remove("filenames_dict.json")
