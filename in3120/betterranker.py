# pylint: disable=missing-module-docstring
# pylint: disable=line-too-long

import math
from .ranker import Ranker
from .corpus import Corpus
from .posting import Posting
from .invertedindex import InvertedIndex


class BetterRanker(Ranker):
    """
    A ranker that does traditional TF-IDF ranking, possibly combining it with
    a static document score (if present).

    The static document score is assumed accessible in a document field named
    "static_quality_score". If the field is missing or doesn't have a value, a
    default value of 0.0 is assumed for the static document score.

    See Section 7.1.4 in https://nlp.stanford.edu/IR-book/pdf/irbookonlinereading.pdf.
    """

    # These values could be made configurable. Hardcode them for now.
    _dynamic_score_weight = 1.0
    _static_score_weight = 1.0
    _static_score_field_name = "static_quality_score"
    _static_score_default_value = 0.0

    def __init__(self, corpus: Corpus, inverted_index: InvertedIndex):
        self._score = 0.0
        self._document_id = None
        self._corpus = corpus
        self._inverted_index = inverted_index

    def reset(self, document_id: int) -> None:
        self._score = 0.0
        self._document_id = document_id

    def update(self, term: str, multiplicity: int, posting: Posting) -> None:
        if posting.document_id != self._document_id:
            raise AssertionError("The documents are not the same")
        
        term_freq = posting.term_frequency * multiplicity
        n = len(self._corpus) # n is the total number of documents in the corpus
        doc_freq = self._inverted_index.get_document_frequency(term)
        idf = math.log(n / doc_freq)
        tf_idf = term_freq * idf

        document = self._corpus.get_document(self._document_id)
        static_score = document.get_field("static_quality_score", 0.0)

        dynamic_score = tf_idf * self._dynamic_score_weight
        weighted_static_score = static_score * self._static_score_weight

        self._score += dynamic_score + weighted_static_score
    
    def evaluate(self) -> float:
        return self._score
