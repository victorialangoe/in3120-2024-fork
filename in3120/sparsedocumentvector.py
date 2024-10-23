# pylint: disable=missing-module-docstring
# pylint: disable=line-too-long

from __future__ import annotations
from typing import Iterable, Iterator, Dict, Tuple, Optional
from math import sqrt
from .sieve import Sieve

class SparseDocumentVector:
    """
    A simple representation of a sparse document vector. The vector space has one dimension
    per vocabulary term, and our representation only lists the dimensions that have non-zero
    values.

    Being able to place text buffers, be they documents or queries, in a vector space and
    thinking of them as point clouds (or, equivalently, as vectors from the origin) enables us
    to numerically assess how similar they are according to some suitable metric. Cosine
    similarity (the inner product of the vectors normalized by their lengths) is a very
    common metric.
    """

    def __init__(self, values: Dict[str, float]):
        # An alternative, effective representation would be as a
        # [(term identifier, weight)] list kept sorted by integer
        # term identifiers. Computing dot products would then be done
        # pretty much in the same way we do posting list AND-scans.
        self._values = {term: weight for term, weight in values.items() if weight != 0.0} # changed because of message on mattermost

        # We cache the length. It might get used over and over, e.g., for cosine
        # computations. A value of None triggers lazy computation.
        self._length : Optional[float] = None

    def __iter__(self):
        return iter(self._values.items())

    def __getitem__(self, term: str) -> float:
        return self._values.get(term, 0.0)

    def __setitem__(self, term: str, weight: float) -> None:
        self._values[term] = weight
        self._length = None

    def __contains__(self, term: str) -> bool:
        return term in self._values

    def __len__(self) -> int:
        """
        Enables use of the built-in len/1 function to count the number of non-zero
        dimensions in the vector. It is not for computing the vector's norm.
        """
        return len(self._values)

    def get_length(self) -> float:
        """
        Returns the length (L^2 norm, also called the Euclidian norm) of the vector.
        """
        sum_of_squares = 0.0

        for _,weight in self._values.items():
            sum_of_squares += weight ** 2

        lenght = sqrt(sum_of_squares)
        return lenght

    def normalize(self) -> None:
        """
        Divides all weights by the length of the vector, thus rescaling it to
        have unit length.
        """
        vector_length = self.get_length()
        if vector_length == 0.0:
            return 
        for term in self._values:
            self._values[term] = self._values[term] / vector_length
        

    def top(self, count: int) -> Iterable[Tuple[str, float]]:
        """
        Returns the top weighted terms, i.e., the "most important" terms and their weights.
        """
        if count < 0:
            raise AssertionError("Count cant be less than 0")
        elif count == 0:
            return []
        sorted_weights = sorted(self._values.items(), key=lambda weight: weight[1], reverse=True)
        return sorted_weights[:count]

    def truncate(self, count: int) -> None:
        """
        Truncates the vector so that it contains no more than the given number of terms,
        by removing the lowest-weighted terms.
        """
        if count < 0:
            raise AssertionError("Count cant be less than 0")
        elif count == 0:
            return []
        
        sorted_weights = sorted(self._values.items(), key=lambda weight: weight[1], reverse=True)
        top_items = dict(sorted_weights[:count])
        self._values = top_items


    def scale(self, factor: float) -> None:
        """
        Multiplies every vector component by the given factor.
        """
        if factor == 0.0:
            return self._values.clear()
        #print("before",self._values)
        for term in self._values:
            self._values[term] = self._values[term] * factor
        #print("after",self._values)
        
    

    def dot(self, other: SparseDocumentVector) -> float:
        """
        Returns the dot product (inner product, scalar product) between this vector
        and the other vector.
        """
        if self._values == {} or other._values == {}:
            return 0
        
        dot_product = sum(self._values[key]*other._values.get(key, 0) for key in self._values) # source: https://stackoverflow.com/questions/33079472/dot-product-with-dictionaries
        return dot_product

    def cosine(self, other: SparseDocumentVector) -> float:
        """
        Returns the cosine of the angle between this vector and the other vector.
        See also https://en.wikipedia.org/wiki/Cosine_similarity.
        """
        dot_product = self.dot(other)
        if dot_product == 0:
            return dot_product
        
        length_vector1 = self.get_length()
        length_vector2 = other.get_length()

        cos = dot_product/ (length_vector1 * length_vector2)
        return cos


    @staticmethod
    def centroid(vectors: Iterator[SparseDocumentVector]) -> SparseDocumentVector:
        """
        Computes the centroid of all the vectors, i.e., the average vector.
        """
        sum_values = {}
        length_counter= 0

        for vector in vectors:
            length_counter = length_counter + 1
            for term, weight in vector._values.items():
                if term in sum_values:
                    sum_values[term] += weight
                else:
                    sum_values[term] = weight

        if length_counter  > 0:
            avg_values = {term: weight_sum / length_counter  for term, weight_sum in sum_values.items()}
        else:
            avg_values = {}

        return SparseDocumentVector(avg_values)