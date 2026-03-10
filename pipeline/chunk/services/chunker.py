from typing import List

import pysbd


class AbstractChunker:
    def __init__(self) -> None:
        self.segmenter = pysbd.Segmenter(language="en", clean=False)

    def split_into_sentences(self, text: str) -> List[str]:
        if not text or not text.strip():
            return []

        sentences = self.segmenter.segment(text.strip())
        return [sent.strip() for sent in sentences if sent and sent.strip()]

    def chunk_abstract_text(self, text: str) -> List[str]:
        return self.split_into_sentences(text)


chunker = AbstractChunker()


def chunk_abstract_text(text: str) -> List[str]:
    return chunker.chunk_abstract_text(text)