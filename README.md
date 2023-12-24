# Ranked retrieval REST API of an Inverted Index using SPIMI technique

## A fully functional REST API to retrieve the most similar songs given a user text query

This project presents a REST API for retrieving the most similar songs based on a user's text query. Leveraging the
Single-Pass In-Memory Indexing (SPIMI) technique, this API indexes data from songs (lyrics, author, album name) to
create a high-speed inverted index. The SPIMI approach enhances indexing efficiency, making the API ideal for rapid and
relevant song retrieval.

Features:

- Efficient SPIMI-based indexing for quick data retrieval.
- Customizable query system tailored for music-related searches.

## Components

### 1. Token Stream Creation

- **Process**: The data undergoes tokenization, normalization, and stemming.

### 2. SPIMI Process

- **Description**: Blocks of user-defined maximum size are created from the token stream and merged into a final index.

### 3. Ranked Retrieval

- **Methodology**: Utilizes tf.idf weights to process the merged index for query use.
- **Benefits**: Ensures relevant and accurately ranked search results.

### 4. REST API

- **Functionality**: Users can retrieve the top 'K' songs that closely match their query.

## Theory source

In order to implement the SPIMI process, the theory given in
in [these docs](https://nlp.stanford.edu/IR-book/html/htmledition/contents-1.html)
by [The Stanford Natural Language Processing Group](https://nlp.stanford.edu/) was used.

## Prerequisites

- Python 3.11
- Basic understanding of REST APIs and Python scripting.

## How to install and use the project

To correctly use this project and see how it works you should follow these steps:

1. **Clone the Project**:
   ```bash
   git clone https://github.com/NicolasArroyo/spimi-ranked-retrieval
   ```

2. **Install Dependencies**:
   ```bash
   cd spimi-ranked-retrieval
   pip install -r requirements.txt
   ```

3. **Environment Setup**:
    - Create a `.env` file with the following content. Adjust `PAGE_SIZE` as needed.

      ```
      PAGE_SIZE=4096 
      ```

4. **Index Generation (Optional)**:
    - Modify the token stream source in `generate_index()` within `index_generator.py`.
    - Run the script: `python3 index_generator.py`
    - *Note*: A pre-loaded index with 16k songs is provided.


5. **Running the API**:
   ```bash
   uvicorn api:app --host 0.0.0.0 --port 3000
   ```

### Usage Examples

- **Query for Artist**:
  ```http request
  POST localhost:3000/api/index
  Content-Type: application/json

  {
    "query": "Weeknd",
    "k": 10
  }
  ```

- **Query for Lyrics**:
  ```http request
  POST localhost:3000/api/index
  Content-Type: application/json

  {
    "query": "Pour some sugar on me",
    "k": 4
  }
  ```

## License

This project is licensed under the MIT License.
