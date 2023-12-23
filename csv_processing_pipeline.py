from text_processing_pipeline import tokenize_line, normalize_tokens, stem_tokens, update_filenames_dict
import csv


def process_files_csv(csv_path, filenames_dict_dir):
    with open(csv_path, "r") as file:
        csv_reader = csv.reader(file)
        next(csv_reader)

        for row in csv_reader:
            if row:
                file_id = update_filenames_dict(filenames_dict_dir, row[0])

                tokens = tokenize_line(','.join(row))
                tokens = normalize_tokens(tokens)
                tokens = stem_tokens(tokens)

                for word in tokens:
                    if word:
                        yield [word, file_id]


