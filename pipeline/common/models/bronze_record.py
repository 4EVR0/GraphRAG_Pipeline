from dataclasses import asdict, dataclass
from typing import Optional


@dataclass
class BronzePaperRecord:
    batch_id: str
    source: str
    searched_ingredient: str
    query_name: str
    alias_list: Optional[str]
    concern_keywords: Optional[str]
    final_query: str
    pmid: Optional[str]
    pmcid: Optional[str]
    doi: Optional[str]
    title: Optional[str]
    abstract_text: Optional[str]
    journal: Optional[str]
    publication_year: Optional[int]
    authors: Optional[str]
    source_url: Optional[str]
    language_code: str = "en"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class BronzeSearchLogRecord:
    batch_id: str
    source: str
    canonical_name: str
    query_name: str
    alias_list: Optional[str]
    concern_keywords: Optional[str]
    final_query: str
    search_limit: int
    pmid_count: int
    collected_at: str

    def to_dict(self) -> dict:
        return asdict(self)