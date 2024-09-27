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
        tokens = list(self.__tokenizer.tokens(buffer)) 
        active_states = []
        yielded_spans = set()

        for _, (token, (start, end)) in enumerate(tokens):
            token = self.__normalizer.normalize(token)
            new_active_states = []
            print(f"on {token} now")
            for current_node, state_start, consumed_tokens in active_states:

                next_node = current_node.consume(token)
                if next_node is not None:
                    if state_start is None:
                        state_start = start

                    new_consumed_tokens = consumed_tokens + [(token, start, end)]

                    if next_node.is_final():
                        match_parts = []
                        prev_end = None
                        for tok, tok_start, tok_end in new_consumed_tokens:
                            if prev_end is not None and tok_start > prev_end:
                                match_parts.append(' ')
                            match_parts.append(tok)
                            prev_end = tok_end
                        match = ''.join(match_parts)

                        surface = buffer[state_start:end]
                        span = (state_start, end)
                        dic = {
                            "surface": ' '.join(surface.split()),
                            "match": match,
                            "meta": next_node.get_meta(),
                            "span": span
                        }
                        if span not in yielded_spans:
                            yielded_spans.add(span)
                            yield dic

                    new_active_states.append((next_node, state_start, new_consumed_tokens))

                    space_after_token = next_node.consume(" ")
                    if space_after_token is not None:
                        new_active_states.append((space_after_token, state_start, new_consumed_tokens))

                else:
                    if current_node.child(" ") is not None:
                        new_active_states.append((current_node, state_start, consumed_tokens))

            root_node = self.__trie.consume(token)
            if root_node:
                state_start = start
                consumed_tokens = [(token, start, end)]

                if root_node.is_final():
                    match = token
                    surface = buffer[start:end]
                    span = (start, end)
                    dic = {
                        "surface": ' '.join(surface.split()),
                        "match": match,
                        "meta": root_node.get_meta(),
                        "span": span
                    }
                    if span not in yielded_spans:
                        yielded_spans.add(span)
                        yield dic

                new_active_states.append((root_node, state_start, consumed_tokens))

                space_after_token = root_node.consume(" ")
                if space_after_token is not None:
                    new_active_states.append((space_after_token, state_start, consumed_tokens))

            active_states = new_active_states




