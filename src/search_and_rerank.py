from sentence_transformers import SentenceTransformer, util
from sentence_transformers import CrossEncoder
import torch
import sqlite3
from aiir_search import make_query, make_statement, query_expansion
from aiir_search import make_union

cross_encoder = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')

def search(query):
    pass


def bm25_search_and_reranking(q1, q2):
    search = sqlite3.connect("cord19_20211019_fts.sqlite")
    cur = search.cursor()

    q1e = query_expansion(q1)
    q1e = make_query(q1e)
    q1e = make_statement(q1e)

    q2e = query_expansion(q2)
    q2e = make_query(q2e)
    q2e = make_statement(q2e)

    res1 = cur.execute(q1e).fetchall()
    res2 = cur.execute(q2e).fetchall()

    res_union = make_union(res1, res2)

    # convert res_union list of tuple into list of dict
    res_union_dict = []
    for item in res_union:
        dict_item = {"title": item[0], "abstract": item[1], 
                     "pubmed_id": item[-2], "score": item[-1]}

        res_union_dict.append(dict_item)

    ce_query = q1 + " " + q2
    
    #  make list of list for cross encoder
    ce_query = [(ce_query, r["title"]) for r in res_union_dict]
    ce_scores = cross_encoder.predict(ce_query)

    for idx in range(len(ce_scores)):
        res_union_dict[idx]["score"] = ce_scores[idx]

    # sort with the new score
    res_sorted = sorted(res_union_dict, key=lambda x: x["score"], reverse=True)
    pubmed_ids = [i['pubmed_id'] for i in res_sorted]

    return pubmed_ids


if __name__ == "__main__":
    pass

    