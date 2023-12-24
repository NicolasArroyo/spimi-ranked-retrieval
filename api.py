import json

from fastapi import FastAPI
from pydantic import BaseModel
from index_generator import generate_index
from index_queries import compute_vector_score


# generate_index()


class Item(BaseModel):
    query: str
    k: int


with open("./index_output/filenames.json", "r", encoding='utf-8') as file:
    filenames_dict = json.load(file)

app = FastAPI()


@app.post("/api/index/")
def post_api(item: Item):
    results = compute_vector_score(item.query, "./index_output/merged_weighted_index.txt", item.k)

    response = {}

    for i, (doc_id, score) in enumerate(results):
        filenames_result = filenames_dict[f"doc{doc_id}"]

        response[f"doc{i}"] = {
            "score": score,
            "id": filenames_result["id"],
            "name": filenames_result["name"],
            "artist": filenames_result["artist"],
            "album_name": filenames_result["album_name"]
        }

    return response
