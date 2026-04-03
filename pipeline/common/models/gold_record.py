from dataclasses import asdict, dataclass
from typing import Optional


@dataclass
class GoldClaimRecord:
    batch_id: str
    claim_key: str
    pmid: str
    chunk_index: int
    section_type: str
    source_sentence: str
    ingredient_name: str
    claim_text: str
    normalized_summary: str
    claim_type: str
    relation: str
    target: str
    target_category: str
    evidence_direction: str
    confidence_score: float
    extraction_method: str
    extractor_version: str
    validator_version: str
    mapping_version: str
    source_start_offset: int
    source_end_offset: int
    title: Optional[str]
    journal: Optional[str]
    publication_year: Optional[int]
    source_url: Optional[str]

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class GoldClaimEffectMapRecord:
    batch_id: str
    claim_key: str
    effect_id: int
    effect_code: str
    effect_name_en: str
    confidence_score: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class GoldClaimConcernMapRecord:
    batch_id: str
    claim_key: str
    concern_id: int
    concern_code: str
    concern_name_en: str
    confidence_score: float

    def to_dict(self) -> dict:
        return asdict(self)