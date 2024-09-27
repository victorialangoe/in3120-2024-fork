# pylint: disable=missing-module-docstring
# pylint: disable=line-too-long
# pylint: disable=too-few-public-methods

from typing import Iterator, Dict, Any, List, Tuple
from .normalizer import Normalizer
from .tokenizer import Tokenizer
from .trie import Trie


class StringFinder:
    """
    Given a trie encoding a dictionary of strings, efficiently finds the subset of strings in the dictionary
    that are also present in a given text buffer. I.e., in a sense computes the "intersection" or "overlap"
    between the dictionary and the text buffer.

    Uses a trie-walk algorithm similar to the Aho-Corasick algorithm with some simplifications and some minor
    NLP extensions. The running time of this algorithm is virtually independent of the size of the dictionary,
    and linear in the length of the buffer we are searching in.

    The tokenizer we use when scanning the input buffer is assumed to be the same as the one that was used
    when adding strings to the trie.
    """

    def __init__(self, trie: Trie, normalizer: Normalizer, tokenizer: Tokenizer):
        self.__trie = trie
        self.__normalizer = normalizer  # The same as was used for trie building.
        self.__tokenizer = tokenizer  # The same as was used for trie building.

    def scan(self, buffer: str) -> Iterator[Dict[str, Any]]:
        """
        Scans the given buffer and finds all dictionary entries in the trie that are also present in the
        buffer. We only consider matches that begin and end on token boundaries.

        The matches, if any, are yielded back to the client as dictionaries having the keys "match" (str),
        "surface" (str), "meta" (Optional[Any]), and "span" (Tuple[int, int]). Note that "match" refers to
        the matching dictionary entry, "surface" refers to the content of the input buffer that triggered the
        match (the surface form), and "span" refers to the exact location in the input buffer where the surface
        form is found. Depending on the normalizer that is used, "match" and "surface" may or may not differ.

        A space-normalized version of the surface form is emitted as "surface", for convenience. Clients
        that require an exact surface form that is not space-normalized can easily reconstruct the desired
        string using the emitted "span" value.

        In a serious application we'd add more lookup/evaluation features, e.g., support for prefix matching,
        support for leftmost-longest matching (instead of reporting all matches), and more.
        """
        tokens = list(self.__tokenizer.tokens(buffer)) 
        active_states = []
        yielded_spans = set()

        for _, (token_str, (start, end)) in enumerate(tokens):
            token_normalized = self.__normalizer.normalize(token_str)
            new_active_states = []

            for current_node, state_start, consumed_tokens in active_states:
                next_node = current_node.consume(token_normalized)
                if next_node is not None:
                    new_consumed_tokens = consumed_tokens + [(token_normalized, start, end)]

                    if next_node.is_final():
                        span = (state_start, end)
                        if span not in yielded_spans:
                            yielded_spans.add(span)
                            # Reconstruct match
                            match_parts = []
                            prev_end = None
                            for tok, tok_start, tok_end in new_consumed_tokens:
                                if prev_end is not None and tok_start > prev_end:
                                    match_parts.append(' ')
                                match_parts.append(tok)
                                prev_end = tok_end
                            match = ''.join(match_parts)
                            surface = buffer[state_start:end]
                            dic = {
                                'surface': ' '.join(surface.strip().split()),
                                'match': match,
                                'meta': next_node.get_meta(),
                                'span': span
                            }
                            yield dic

                    new_active_states.append((next_node, state_start, new_consumed_tokens))
                    space_node = next_node.consume(" ")
                    if space_node is not None:
                        new_active_states.append((space_node, state_start, new_consumed_tokens))

                else:
                    space_node = current_node.consume(" ")
                    if space_node is not None:
                        new_active_states.append((space_node, state_start, consumed_tokens))

            root_node = self.__trie.consume(token_normalized)
            if root_node is not None:
                state_start = start
                consumed_tokens = [(token_normalized, start, end)]

                if root_node.is_final():
                    span = (state_start, end)
                    if span not in yielded_spans:
                        yielded_spans.add(span)
                        dic = {
                            'surface': ' '.join(buffer[start:end].strip().split()),
                            'match': token_normalized,
                            'meta': root_node.get_meta(),
                            'span': span
                        }
                        yield dic

                new_active_states.append((root_node, state_start, consumed_tokens))

                space_after_token = root_node.consume(" ")
                if space_after_token is not None:
                    new_active_states.append((space_after_token, state_start, consumed_tokens))

            active_states = new_active_states





