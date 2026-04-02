from dataclasses import asdict, dataclass
from typing import Optional


@dataclass
class SilverPaperRecord:
    batch_id: str
    pmid: str
    title: Optional[str]
    abstract_text: Optional[str]
    journal: Optional[str]
    publication_year: Optional[int]
    source_url: Optional[str]
    searched_ingredient_count: int
    searched_ingredients: Optional[str]

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SilverChunkRecord:
    batch_id: str
    pmid: str
    chunk_index: int
    section_type: str
    chunk_text: str
    char_count: int
    token_count_approx: int
    source_start_offset: int
    source_end_offset: int
    chunk_version: str
    title: Optional[str]
    journal: Optional[str]
    publication_year: Optional[int]
    source_url: Optional[str]

    def to_dict(self) -> dict:
        return asdict(self)