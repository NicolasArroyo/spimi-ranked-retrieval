import nltk
import math


def compute_vector_score(query, merged_weighted_index_path, k):
    porter_stemmer = nltk.PorterStemmer()

    index = {}
    with open(merged_weighted_index_path, "r") as file:
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
