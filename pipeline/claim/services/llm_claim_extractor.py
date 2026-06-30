import hashlib
import json
import os
import sqlite3
from pathlib import Path
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


SYSTEM_PROMPT = """You are a strict dermatology/cosmetic-ingredient claim extraction system.

You will read exactly one sentence from a paper abstract and extract only explicit, sentence-level claims.

Return JSON only.

Hard rules:
1. Extract claims only when the ingredient claim is explicit in the sentence.
2. Do not infer from the whole abstract.
3. Do not use background/method framing as evidence.
4. If no valid claim exists, return {"claims": []}.
5. Use only the provided enum values.
6. Keep evidence_text exactly equal to the input sentence.
7. Only extract ingredient claims relevant to cosmetic/skin outcomes.

Keep claims only for targets like:
- hyperpigmentation
- melasma
- skin barrier function
- hydration
- dry skin
- irritation
- erythema
- redness
- acne
- sebum production
- photoaging
- wrinkles
- UV-induced immunosuppression
- post-inflammatory hyperpigmentation
- tolerability / tolerance

Reject claims if the target is mainly about:
- carcinoma, cancer, tumor
- surgery, perioperative bleeding, blood loss
- microemulsion, nanoemulsion, particle size, release profile
- drug delivery / formulation optimization
- cell migration, dermal papilla cells, gene expression only
- generic therapy/treatment effectiveness
- non-skin systemic outcomes

Relation guidance:
- improves / reduces / prevents are preferred for efficacy
- is_well_tolerated_for or is_safe_for for tolerability/safety
- increases should be used only for clearly cosmetic targets such as hydration, elasticity, or tolerance
- do not output stimulates for cell migration or other lab-mechanism-only targets

If the sentence contains uncertainty words like 'may', 'might', 'appears', 'suggests', set hedging=true.
If the sentence contains negation like 'did not', 'no significant', 'not associated', set negation=true.
"""


class LLMClaimExtractor:
    def __init__(
        self,
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        temperature: float = 0.0,
        max_completion_tokens: int = 1200,
        cache_path: Optional[str] = None,
    ) -> None:
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
        self.temperature = temperature
        self.max_completion_tokens = max_completion_tokens

        resolved_api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not resolved_api_key:
            raise RuntimeError("OPENAI_API_KEY is not set.")

        self.client = OpenAI(api_key=resolved_api_key)
        resolved_cache_path = cache_path or os.getenv(
            "LLM_CACHE_PATH",
            "gold/cache/llm_claims.sqlite3",
        )
        self.cache_path = Path(resolved_cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache = sqlite3.connect(self.cache_path)
        self.cache.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_response_cache (
                cache_key TEXT PRIMARY KEY,
                model TEXT NOT NULL,
                response TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.cache.commit()

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
        cache_payload = "\n".join(
            [
                self.model,
                SYSTEM_PROMPT,
                json.dumps(CLAIMS_JSON_SCHEMA, sort_keys=True),
                user_prompt,
            ]
        )
        cache_key = hashlib.sha256(cache_payload.encode("utf-8")).hexdigest()
        cached = self.cache.execute(
            "SELECT response FROM llm_response_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
        if cached:
            return str(cached[0])

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
            content = '{"claims": []}'
        self.cache.execute(
            """
            INSERT OR REPLACE INTO llm_response_cache (cache_key, model, response)
            VALUES (?, ?, ?)
            """,
            (cache_key, self.model, content),
        )
        self.cache.commit()
        return content

    def _parse_payload(self, raw_payload: str) -> List[Dict[str, Any]]:
        try:
            obj = json.loads(raw_payload)
        except json.JSONDecodeError:
            return []

        claims = obj.get("claims", [])
        if not isinstance(claims, list):
            return []

        cleaned_claims = []
        for claim in claims:
            if not isinstance(claim, dict):
                continue

            target = str(claim.get("target", "")).strip().lower()
            if any(
                blocked in target
                for blocked in [
                    "carcinoma",
                    "cancer",
                    "tumor",
                    "tumour",
                    "microemulsion",
                    "nanoemulsion",
                    "particle size",
                    "release profile",
                    "drug delivery",
                    "cell migration",
                    "dermal papilla",
                    "gene expression",
                    "therapy",
                    "treatment",
                    "blood loss",
                    "perioperative",
                ]
            ):
                continue

            cleaned_claims.append(claim)

        return cleaned_claims


llm_extractor = LLMClaimExtractor()
