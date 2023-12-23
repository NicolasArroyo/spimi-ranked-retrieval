from fastapi import FastAPI
from pydantic import BaseModel
from spimi_indexer import initialize_index, compute_vector_score, initialize_index_csv
import json

initialize_index_csv("./blocks", "./merged_index.txt", "./filenames_dict.json")


class Item(BaseModel):
    query: str
    k: int


with open("./filenames_dict.json", "r", encoding='utf-8') as file:
    filenames_dict = json.load(file)

app = FastAPI()


@app.post("/api/index/")
def post_api(item: Item):
    results = compute_vector_score(item.query, "./merged_index_tfidf.txt", item.k)

    response = {}
    for doc_id, score in results:
        response[filenames_dict[f"doc{doc_id}"]] = score

    return response
