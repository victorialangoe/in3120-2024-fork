# pylint: disable=missing-module-docstring
# pylint: disable=line-too-long

import sys
from bisect import bisect_left
from itertools import takewhile
from typing import Any, Dict, Iterator, Iterable, Tuple, List
from collections import Counter
from .corpus import Corpus
from .normalizer import Normalizer
from .tokenizer import Tokenizer

class SuffixArray:
    """
    A simple suffix array implementation. Allows us to conduct efficient substring searches.
    The prefix of a suffix is an infix!

    In a serious application we'd make use of least common prefixes (LCPs), pay more attention
    to memory usage, and add more lookup/evaluation features.
    """

    def __init__(self, corpus: Corpus, fields: Iterable[str], normalizer: Normalizer, tokenizer: Tokenizer):
        self.__corpus = corpus
        self.__normalizer = normalizer
        self.__tokenizer = tokenizer
        self.__haystack: List[Tuple[int, str]] = []  # The (<document identifier>, <searchable content>) pairs.
        self.__suffixes: List[Tuple[int, int]] = []  # The sorted (<haystack index>, <start offset>) pairs.
        self.__build_suffix_array(fields)  # Construct the haystack and the suffix array itself.

    def __build_suffix_array(self, fields: Iterable[str]) -> None:
        """
        Builds a simple suffix array from the set of named fields in the document collection.
        The suffix array allows us to search across all named fields in one go.
        """
        for document in self.__corpus: 
            doc_id = document.document_id 
            full_content = " ".join(document[field_name] for field_name in fields) 
            normalized_content = self.__normalize(full_content)
            self.__haystack.append((doc_id, normalized_content))

        for doc_idx, (_, content) in enumerate(self.__haystack):
            for i in range(len(content)):
                if i == 0 or content[i - 1] == " ": # if character is *space* before i, we know a new word is the start of i
                    self.__suffixes.append((doc_idx, i))

        self.__suffixes.sort(key=lambda x: self.__haystack[x[0]][1][x[1]:]) # said we could have a naïve construft of our suffixarray 


    def __normalize(self, buffer: str) -> str:
        """
        Produces a normalized version of the given string. Both queries and documents need to be
        identically processed for lookups to succeed.
        """
        normalized_content = self.__normalizer.canonicalize(buffer)
        normalized_content = self.__tokenizer.strings(normalized_content)
        normalized_content = [self.__normalizer.normalize(t) for t in normalized_content]  # using the same as get_terms from last assignment
        normalized_content = " ".join(normalized_content) # instead of returning list, return as str 
        return normalized_content

    def __binary_search(self, needle: str) -> int:
        """
        Does a binary search to find the first occurrence of a given normalized query
        in the suffix array.
        """

        return bisect_left(self.__suffixes, needle, key=lambda x: self.__haystack[x[0]][1][x[1]:])



    def evaluate(self, query: str, options: dict) -> Iterator[Dict[str, Any]]:
        """
        Evaluates the given query, doing a "phrase prefix search".  E.g., for a supplied query phrase like
        "to the be", we return documents that contain phrases like "to the bearnaise", "to the best",
        "to the behemoth", and so on. I.e., we require that the query phrase starts on a token boundary in the
        document, but it doesn't necessarily have to end on one.

        The matching documents are ranked according to how many times the query substring occurs in the document,
        and only the "best" matches are yielded back to the client. Ties are resolved arbitrarily.

        The client can supply a dictionary of options that controls this query evaluation process: The maximum
        number of documents to return to the client is controlled via the "hit_count" (int) option.

        The results yielded back to the client are dictionaries having the keys "score" (int) and
        "document" (Document).
        """

        tokenized_query = self.__normalize(query)
        print(f"Normalized query: {tokenized_query}")

        if not tokenized_query:
            return

        counter = Counter()

        start_idx = self.__binary_search(tokenized_query)
        print("QUERY FOR THIS ROUND", query)
        print("start idx for this round:", start_idx)

        for i in range(start_idx, len(self.__suffixes)):
            doc_id, pos = self.__suffixes[i]
            tokenized_content = self.__haystack[doc_id][1]
            suffix_tokens = tokenized_content[pos:]
            
            if len(tokenized_query) == 1:
                if suffix_tokens[0].startswith(tokenized_query[0]):
                    counter[doc_id] += 1
                else:
                    break 

            else:
                if suffix_tokens[:len(tokenized_query)] == tokenized_query:
                    counter[doc_id] += 1
                else:
                    break 

        for doc_id, score in counter.most_common(options.get("hit_count", 5)):
            results = {
                "document": self.__corpus.get_document(doc_id),
                "score": score
            }
            yield results