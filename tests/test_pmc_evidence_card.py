"""Offline tests for source-linked PMC evidence cards."""

import pytest

from pipeline.fulltext.evidence_card import (
    EvidenceCard,
    EvidenceCardError,
    render_cautious_summary,
    validate_card,
)


PASSAGES = [
    {"passage_id": "p1", "text": "The formulation contained salicylic acid, glycolic acid, and niacinamide."},
    {"passage_id": "p2", "text": "Forty two participants with acne used the gel for twelve weeks."},
    {"passage_id": "p3", "text": "The combined gel reduced acne lesions after twelve weeks of use."},
    {"passage_id": "p4", "text": "We did not observe significant changes in sebum production after treatment."},
]


def card_data() -> dict:
    return {
        "pmid": "123",
        "population": "여드름이 있는 42명",
        "population_source": {"passage_id": "p2", "quote": PASSAGES[1]["text"]},
        "intervention_type": "multi_ingredient_formulation",
        "intervention_description": "살리실산 등을 포함한 복합 젤",
        "intervention_source": {"passage_id": "p1", "quote": PASSAGES[0]["text"]},
        "tested_ingredients": [
            {"name": "salicylic acid", "source": {"passage_id": "p1", "quote": PASSAGES[0]["text"]}},
            {"name": "glycolic acid", "source": {"passage_id": "p1", "quote": PASSAGES[0]["text"]}},
        ],
        "attribution_scope": "combination_or_formulation",
        "outcomes": [{
            "measure": "여드름 병변",
            "finding": "improved",
            "observed_result": "12주 후 감소",
            "source": {"passage_id": "p3", "quote": PASSAGES[2]["text"]},
        }],
        "limitations": [],
    }


def test_valid_formulation_card_has_explicit_boundary():
    card = EvidenceCard.model_validate(card_data())
    validate_card(card, "123", PASSAGES)
    assert "단독 효능으로 해석할 수 없습니다" in render_cautious_summary(card)


@pytest.mark.parametrize("field,value", [
    ("attribution_scope", "single_ingredient"),
    ("intervention_type", "chemical_peel"),
])
def test_rejects_inconsistent_attribution(field, value):
    data = card_data()
    data[field] = value
    with pytest.raises(EvidenceCardError):
        validate_card(EvidenceCard.model_validate(data), "123", PASSAGES)


def test_rejects_hallucinated_quote_and_passage_id():
    data = card_data()
    data["outcomes"][0]["source"]["quote"] = "The gel completely cured acne in all participants."
    with pytest.raises(EvidenceCardError, match="Quote not found"):
        validate_card(EvidenceCard.model_validate(data), "123", PASSAGES)
    data["outcomes"][0]["source"]["passage_id"] = "not-a-passage"
    with pytest.raises(EvidenceCardError, match="Unknown passage ID"):
        validate_card(EvidenceCard.model_validate(data), "123", PASSAGES)


def test_rejects_cross_paper_card():
    with pytest.raises(EvidenceCardError, match="PMID mismatch"):
        validate_card(EvidenceCard.model_validate(card_data()), "999", PASSAGES)


def test_rejects_empty_outcome():
    data = card_data()
    data["outcomes"] = []
    with pytest.raises(EvidenceCardError, match="measured outcome"):
        validate_card(EvidenceCard.model_validate(data), "123", PASSAGES)


def test_null_result_is_preserved_in_summary():
    data = card_data()
    data["outcomes"][0]["finding"] = "no_significant_change"
    data["outcomes"][0]["observed_result"] = "통계적으로 유의한 변화가 없었음"
    data["outcomes"][0]["source"] = {"passage_id": "p4", "quote": PASSAGES[3]["text"]}
    card = EvidenceCard.model_validate(data)
    validate_card(card, "123", PASSAGES)
    assert "통계적으로 유의한 변화가 없었음" in render_cautious_summary(card)


def test_rejects_ingredient_name_absent_from_quote():
    data = card_data()
    data["tested_ingredients"][0]["name"] = "azelaic acid"
    with pytest.raises(EvidenceCardError, match="Ingredient name not found"):
        validate_card(EvidenceCard.model_validate(data), "123", PASSAGES)
