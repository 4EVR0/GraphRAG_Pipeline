from dataclasses import dataclass
from typing import Optional


@dataclass
class PaperRecord:
    title: Optional[str]
    doi: Optional[str]
    pmid: Optional[str]
    pmcid: Optional[str]
    journal: Optional[str]
    publication_year: Optional[int]
    authors: Optional[str]
    abstract_text: Optional[str]
    study_type: Optional[str]
    evidence_level: Optional[str]
    source_db: str
    source_url: Optional[str]
    language_code: str = "en"

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "doi": self.doi,
            "pmid": self.pmid,
            "pmcid": self.pmcid,
            "journal": self.journal,
            "publication_year": self.publication_year,
            "authors": self.authors,
            "abstract_text": self.abstract_text,
            "study_type": self.study_type,
            "evidence_level": self.evidence_level,
            "source_db": self.source_db,
            "source_url": self.source_url,
            "language_code": self.language_code,
        }