import json
import os
from typing import Any, Dict, List, Optional

from openai import OpenAI


ALLOWED_CLAIM_TYPES = {"efficacy", "safety", "mechanism"}
ALLOWED_RELATIONS = {
    "improves",
    "reduces",
    "prevents",
    "increases",
    "is_safe_for",
    "is_well_tolerated_for",
    "causes",
    "does_not_cause",
    "inhibits",
    "stimulates",
    "regulates",
    "modulates",
}
ALLOWED_TARGET_CATEGORIES = {"effect", "concern", "mechanism_target"}
ALLOWED_EVIDENCE_DIRECTIONS = {"supports", "refutes", "neutral"}
ALLOWED_STUDY_CONTEXTS = {
    "human_topical",
    "human_oral",
    "human_intradermal",
    "in_vitro",
    "animal",
    "review",
    "unknown",
}

CLAIMS_JSON_SCHEMA: Dict[str, Any] = {
    "name": "claim_extraction_result",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "claims": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "ingredient": {"type": "string"},
                        "claim_type": {
                            "type": "string",
                            "enum": sorted(ALLOWED_CLAIM_TYPES),
                        },
                        "relation": {
                            "type": "string",
                            "enum": sorted(ALLOWED_RELATIONS),
                        },
                        "target": {"type": "string"},
                        "target_category": {
                            "type": "string",
                            "enum": sorted(ALLOWED_TARGET_CATEGORIES),
                        },
                        "evidence_direction": {
                            "type": "string",
                            "enum": sorted(ALLOWED_EVIDENCE_DIRECTIONS),
                        },
                        "evidence_text": {"type": "string"},
                        "study_context": {
                            "type": "string",
                            "enum": sorted(ALLOWED_STUDY_CONTEXTS),
                        },
                        "hedging": {"type": "boolean"},
                        "negation": {"type": "boolean"},
                        "confidence": {
                            "type": "number",
                            "minimum": 0.0,
                            "maximum": 1.0,
                        },
                    },
                    "required": [
                        "ingredient",
                        "claim_type",
                        "relation",
                        "target",
                        "target_category",
                        "evidence_direction",
                        "evidence_text",
                        "study_context",
                        "hedging",
                        "negation",
                        "confidence",
                    ],
                },
            }
        },
        "required": ["claims"],
    },
}

SYSTEM_PROMPT = """You are a precise biomedical claim extraction system for dermatology and cosmetic science papers.

Your task:
- Read exactly one sentence from a paper abstract.
- Extract ONLY explicit ingredient-related claims stated in that sentence.
- Return JSON only, matching the provided schema exactly.

Important rules:
1. Do not infer or hallucinate.
2. Do not summarize the whole paper.
3. Do not extract claims from background, objective, methods, or generic review framing unless the sentence itself explicitly states an ingredient-related claim.
4. If there is no valid claim, return {"claims": []}.
5. A single sentence may contain multiple claims. Return all valid claims.
6. The ingredient must be explicitly identifiable from the sentence or strongly supported by the provided candidate list.
7. Keep evidence_text as the original sentence text, unchanged.
8. Use only the allowed enum values.
9. If the sentence contains weak uncertainty such as 'may', 'might', 'appears', 'suggests', set hedging=true.
10. If the sentence contains negation such as 'did not', 'no significant', 'not associated', set negation=true.
11. If the sentence compares to another treatment, only extract the explicit claim supported by the sentence.
12. If no ingredient-related claim is present, return {"claims": []}.
"""


class LLMClaimExtractor:
    def __init__(
        self,
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        temperature: float = 0.0,
        max_completion_tokens: int = 1200,
    ) -> None:
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
        self.temperature = temperature
        self.max_completion_tokens = max_completion_tokens

        resolved_api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not resolved_api_key:
            raise RuntimeError("OPENAI_API_KEY is not set.")

        self.client = OpenAI(api_key=resolved_api_key)

    def extract(
        self,
        sentence: str,
        ingredient_candidates: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        sentence = sentence.strip()
        if not sentence:
            return []

        raw_payload = self._call_llm(
            sentence=sentence,
            ingredient_candidates=ingredient_candidates or [],
        )
        return self._parse_payload(raw_payload)

    def _build_user_prompt(
        self,
        sentence: str,
        ingredient_candidates: List[str],
    ) -> str:
        candidate_block = ", ".join(ingredient_candidates) if ingredient_candidates else "None"

        return f"""Extract structured claims from the sentence below.

Allowed claim_type:
- efficacy
- safety
- mechanism

Allowed relation:
- improves
- reduces
- prevents
- increases
- is_safe_for
- is_well_tolerated_for
- causes
- does_not_cause
- inhibits
- stimulates
- regulates
- modulates

Allowed target_category:
- effect
- concern
- mechanism_target

Allowed evidence_direction:
- supports
- refutes
- neutral

Allowed study_context:
- human_topical
- human_oral
- human_intradermal
- in_vitro
- animal
- review
- unknown

Ingredient candidates from upstream matcher:
{candidate_block}

Sentence:
{sentence}

Return JSON only.
"""

    def _call_llm(
        self,
        sentence: str,
        ingredient_candidates: List[str],
    ) -> str:
        user_prompt = self._build_user_prompt(sentence, ingredient_candidates)

        response = self.client.chat.completions.create(
            model=self.model,
            temperature=self.temperature,
            max_completion_tokens=self.max_completion_tokens,
            response_format={
                "type": "json_schema",
                "json_schema": CLAIMS_JSON_SCHEMA,
            },
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        )

        content = response.choices[0].message.content
        if not content:
            return '{"claims": []}'
        return content

    def _parse_payload(self, raw_payload: str) -> List[Dict[str, Any]]:
        try:
            obj = json.loads(raw_payload)
        except json.JSONDecodeError:
            return []

        claims = obj.get("claims", [])
        if not isinstance(claims, list):
            return []
        return [claim for claim in claims if isinstance(claim, dict)]


llm_extractor = LLMClaimExtractor()