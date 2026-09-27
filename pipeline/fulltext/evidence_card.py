"""Source-linked evidence cards for the isolated PMC full-text experiment.

The model proposes observations; validation checks source links and attribution
scope before a card can be used for answer drafting. This is not a production
claim graph or a substitute for human review of scientific interpretation.
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict


class SourceQuote(BaseModel):
    model_config = ConfigDict(extra="forbid")

    passage_id: str
    quote: str


class IngredientMention(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    source: SourceQuote


class MeasuredOutcome(BaseModel):
    model_config = ConfigDict(extra="forbid")

    measure: str
    finding: Literal["improved", "no_significant_change", "worsened", "unclear"]
    observed_result: str
    source: SourceQuote


class EvidenceCard(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pmid: str
    population: str
    population_source: SourceQuote
    intervention_type: Literal[
        "single_ingredient", "multi_ingredient_formulation", "chemical_peel", "other"
    ]
    intervention_description: str
    intervention_source: SourceQuote
    tested_ingredients: list[IngredientMention]
    attribution_scope: Literal["single_ingredient", "combination_or_formulation", "procedure", "uncertain"]
    outcomes: list[MeasuredOutcome]
    limitations: list[SourceQuote]


class EvidenceCardError(ValueError):
    """The proposed card is not safely linked to its source passages."""


def validate_card(card: EvidenceCard, pmid: str, passages: list[dict]) -> None:
    """Reject missing/mismatched quotes and unsupported single-ingredient attribution."""
    if card.pmid != pmid:
        raise EvidenceCardError("PMID mismatch")
    indexed = {item["passage_id"]: item["text"] for item in passages}
    if len(indexed) != len(passages):
        raise EvidenceCardError("Duplicate source passage ID")

    sources = [card.population_source, card.intervention_source]
    sources.extend(item.source for item in card.tested_ingredients)
    sources.extend(item.source for item in card.outcomes)
    sources.extend(card.limitations)
    for source in sources:
        if len(source.quote) < 20:
            raise EvidenceCardError("Source quote is too short")
        text = indexed.get(source.passage_id)
        if text is None:
            raise EvidenceCardError(f"Unknown passage ID: {source.passage_id}")
        if source.quote not in text:
            raise EvidenceCardError(f"Quote not found in passage: {source.passage_id}")
    for ingredient in card.tested_ingredients:
        if ingredient.name.casefold() not in ingredient.source.quote.casefold():
            raise EvidenceCardError(f"Ingredient name not found in cited quote: {ingredient.name}")
    for outcome in card.outcomes:
        if outcome.finding == "no_significant_change" and not re.search(
            r"(?:no|not|without|did not observe)\s+(?:statistically\s+)?significant|p\s*[≥=>]\s*0\.05",
            outcome.source.quote,
            flags=re.IGNORECASE,
        ):
            raise EvidenceCardError("No-significant-change finding lacks a matching source phrase")

    if not card.outcomes:
        raise EvidenceCardError("At least one measured outcome is required")
    if card.intervention_type == "single_ingredient":
        if card.attribution_scope not in {"single_ingredient", "uncertain"}:
            raise EvidenceCardError("Single-ingredient intervention has inconsistent attribution")
        if len(card.tested_ingredients) != 1:
            raise EvidenceCardError("Single-ingredient intervention must name exactly one ingredient")
    elif card.attribution_scope == "single_ingredient":
        raise EvidenceCardError("Non-single-ingredient intervention cannot imply single-ingredient efficacy")
    if card.intervention_type == "chemical_peel" and card.attribution_scope != "procedure":
        raise EvidenceCardError("Chemical peel outcomes must be attributed to the procedure")
    if card.intervention_type == "multi_ingredient_formulation" and card.attribution_scope != "combination_or_formulation":
        raise EvidenceCardError("Multi-ingredient outcomes must be attributed to the formulation")
    if card.intervention_type == "multi_ingredient_formulation" and len(card.tested_ingredients) < 2:
        raise EvidenceCardError("Multi-ingredient intervention must cite at least two ingredients")


def render_cautious_summary(card: EvidenceCard) -> str:
    """Render a compact, deterministic boundary statement, not a product claim."""
    outcome_text = "; ".join(f"{item.measure}: {item.observed_result}" for item in card.outcomes)
    if card.attribution_scope == "single_ingredient":
        boundary = "이 연구의 결과는 해당 성분을 단독으로 시험한 조건에 한정됩니다."
    elif card.attribution_scope == "procedure":
        boundary = "시술 전체의 결과이므로 특정 성분의 단독 효능으로 해석할 수 없습니다."
    elif card.attribution_scope == "combination_or_formulation":
        boundary = "복합 제형의 결과이므로 특정 성분의 단독 효능으로 해석할 수 없습니다."
    else:
        boundary = "이 자료만으로 특정 성분의 단독 효과를 확정할 수 없습니다."
    return f"연구 대상: {card.population}. 시험 조건: {card.intervention_description}. 관찰 결과: {outcome_text}. {boundary}"
