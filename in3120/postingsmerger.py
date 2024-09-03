# pylint: disable=missing-module-docstring

from typing import Iterator
from .posting import Posting 


class PostingsMerger:
    """
    Utility class for merging posting lists.

    It is currently left unspecified what to do with the term frequency field
    in the returned postings when document identifiers overlap. Different
    approaches are possible, e.g., an arbitrary one of the two postings could
    be returned, or the posting having the smallest/largest term frequency, or
    a new one that produces an averaged value, or something else.
    """

    @staticmethod
    def intersection(iter1: Iterator[Posting], iter2: Iterator[Posting]) -> Iterator[Posting]:
        """
        A generator that yields a simple AND(A, B) of two posting
        lists A and B, given iterators over these.

        The posting lists are assumed sorted in increasing order according
        to the document identifiers.
        """

        try:
            pointer1 = next(iter1)
            pointer2 = next(iter2)
        except StopIteration:
            return
        
        while True:
            try: 
                if pointer1.document_id == pointer2.document_id:
                    yield pointer1
                    pointer1 = next(iter1)
                    pointer2 = next(iter2)
                elif pointer1.document_id < pointer2.document_id:
                    pointer1 = next(iter1)
                else:
                    pointer2 = next(iter2)
            except StopIteration:
                break

       

    @staticmethod
    def union(iter1: Iterator[Posting], iter2: Iterator[Posting]) -> Iterator[Posting]:
        """
        A generator that yields a simple OR(A, B) of two posting
        lists A and B, given iterators over these.

        The posting lists are assumed sorted in increasing order according
        to the document identifiers.
        """
        has_seen = set()  

        try:
            pointer1 = next(iter1)
        except StopIteration:
            yield from iter2  
            return
        
        try:
            pointer2 = next(iter2)
        except StopIteration:
            yield pointer1 
            yield from iter1 
            return
        
        while True: 
            try:
                if pointer1.document_id == pointer2.document_id:
                    if pointer1.document_id not in has_seen:
                        yield pointer1
                        has_seen.add(pointer1.document_id)
                    pointer1 = next(iter1)
                    pointer2 = next(iter2)
                elif pointer1.document_id < pointer2.document_id:
                    if pointer1.document_id not in has_seen:
                        yield pointer1
                        has_seen.add(pointer1.document_id)
                    pointer1 = next(iter1)
                else:
                    if pointer2.document_id not in has_seen:
                        yield pointer2
                        has_seen.add(pointer2.document_id)
                    pointer2 = next(iter2)
            except StopIteration:
                break
        
        if pointer1.document_id not in has_seen:
            yield pointer1
            has_seen.add(pointer1.document_id)
        yield from (p for p in iter1 if p.document_id not in has_seen)
        
        if pointer2.document_id not in has_seen:
            yield pointer2
            has_seen.add(pointer2.document_id)
        yield from (p for p in iter2 if p.document_id not in has_seen)

    @staticmethod
    def difference(iter1: Iterator[Posting], iter2: Iterator[Posting]) -> Iterator[Posting]:
        """
        A generator that yields a simple ANDNOT(A, B) of two posting
        lists A and B, given iterators over these.

        The posting lists are assumed sorted in increasing order according
        to the document identifiers.
        """        
        try:
            pointer1 = next(iter1)
        except StopIteration:
            return # only return since then iter1 has nothing in common to iter2
        
        try:
            pointer2 = next(iter2)
        except StopIteration:
            yield pointer1  
            yield from iter1 
            return
        
        last_yielded = None

        while True:
            try:
                if pointer1.document_id < pointer2.document_id:
                    last_yielded = pointer1
                    yield pointer1
                    pointer1 = next(iter1)
                elif pointer1.document_id > pointer2.document_id:
                    pointer2 = next(iter2)
                else:
                    pointer1 = next(iter1)
                    pointer2 = next(iter2)
            except StopIteration:
                break

        if last_yielded is None or last_yielded.document_id != pointer1.document_id:
            yield pointer1

        try:
            while True:
                pointer1 = next(iter1)
                print(f"yielding remaining {pointer1.document_id}")
                yield pointer1
        except StopIteration:
            pass
