# pylint: disable=missing-module-docstring
# pylint: disable=line-too-long
# pylint: disable=too-few-public-methods
# pylint: disable=too-many-locals

from collections import Counter
from typing import Iterator, Dict, Any
from .sieve import Sieve
from .ranker import Ranker
from .corpus import Corpus
from .invertedindex import InvertedIndex


class SimpleSearchEngine:
    """
    Realizes a simple query evaluator that efficiently performs N-of-M matching over an inverted index.
    I.e., if the query contains M unique query terms, each document in the result set should contain at
    least N of these m terms. For example, 2-of-3 matching over the query 'orange apple banana' would be
    logically equivalent to the following predicate:

       (orange AND apple) OR (orange AND banana) OR (apple AND banana)
       
    Note that N-of-M matching can be viewed as a type of "soft AND" evaluation, where the degree of match
    can be smoothly controlled to mimic either an OR evaluation (1-of-M), or an AND evaluation (M-of-M),
    or something in between.

    The evaluator uses the client-supplied ratio T = N/M as a parameter as specified by the client on a
    per query basis. For example, for the query 'john paul george ringo' we have M = 4 and a specified
    threshold of T = 0.7 would imply that at least 3 of the 4 query terms have to be present in a matching
    document.
    """

    def __init__(self, corpus: Corpus, inverted_index: InvertedIndex):
        self.__corpus = corpus
        self.__inverted_index = inverted_index

    def evaluate(self, query: str, options: Dict[str, Any], ranker: Ranker) -> Iterator[Dict[str, Any]]:
        """
        Evaluates the given query, doing N-out-of-M ranked retrieval. I.e., for a supplied query having M
        unique terms, a document is considered to be a match if it contains at least N <= M of those terms.

        The matching documents, if any, are ranked by the supplied ranker, and only the "best" matches are yielded
        back to the client as dictionaries having the keys "score" (float) and "document" (Document).

        The client can supply a dictionary of options that controls the query evaluation process: The value of
        N is inferred from the query via the "match_threshold" (float) option, and the maximum number of documents
        to return to the client is controlled via the "hit_count" (int) option.
        """
        terms = list(self.__inverted_index.get_terms(query))
        term_counts = Counter(terms)
        terms = list(term_counts)
        m = len(terms)
        treshold = options.get("match_threshold", 0.75) #0,75 as default
        n =max(1, min(m, int(treshold * m)))
        max_documents=options.get("hit_count")
        sieve= Sieve(max_documents)
        posting_iterators = [self.__inverted_index.get_postings_iterator(term) for term in terms]#retrieves posting lists for each term in the query
        postings = [next(iterator, None) for iterator in posting_iterators] #postings stores the first posting from each terms posting list

        def has_remaining_postings(postings_list): # to check if we have more postings to process, if not then break the loop
            return any(posting is not None for posting in postings_list)

        while has_remaining_postings(postings):
            matching_indices = set()
            min_doc_id = min(posting.document_id for posting in postings if posting is not None)

            for idx, posting in enumerate(postings):
                if posting is not None and posting.document_id == min_doc_id:
                    matching_indices.add(idx)

            if len(matching_indices) >= n:
                doc_id = min_doc_id
                ranker.reset(doc_id)
                for idx in matching_indices:
                    term = terms[idx]
                    multiplicity = term_counts[term]
                    posting = postings[idx]
                    ranker.update(term, multiplicity, posting)
                score = ranker.evaluate()
                sieve.sift(score, doc_id)

            for idx in matching_indices:
                try:
                    postings[idx] = next(posting_iterators[idx])
                except StopIteration:
                    postings[idx] = None

        for score, doc_id in sieve.winners():
            yield {"score": score, "document": self.__corpus.get_document(doc_id)}



   
       