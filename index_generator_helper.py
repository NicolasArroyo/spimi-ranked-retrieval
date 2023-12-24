import sys
import os
import shutil


def parse_line(line):
    if line[0] == ",":
        line = line[1:]
    parts = line.strip().split(',')
    word = parts[0]
    # print(f"line -> {line}")
    # print(f"parts -> {parts}")
    # print(f"word -> {word}")
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


def clear_folder(folder_path):
    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
