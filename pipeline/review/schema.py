"""근거 검수 판정 형식과 프롬프트 (#49).

프롬프트·스키마가 바뀌면 prompt_sha가 바뀌고, 같은 PMID도 다시 판정한다.
"""
import hashlib
import json

ATTRIBUTIONS = ("single", "combination", "comparator_only")
ROUTES = ("topical_leave_on", "topical_rinse_off", "peel", "oral", "procedure")
# #49 시범 범위의 study_type에 동물 연구와 기타(분석법·제형 개발 등)를 더했다.
STUDY_TYPES = ("rct", "cohort", "case_series", "in_vitro", "animal", "review", "other")
DIRECTIONS = ("improves", "worsens", "no_difference", "unclear")
SOURCES = ("abstract", "pmc_fulltext")
EFFECT_CODES = (
    "ANTI_INFLAMMATORY", "SOOTHING", "BARRIER_REPAIR", "HYDRATING", "MOISTURE_RETENTION",
    "SEBUM_REGULATION", "KERATOLYTIC", "COMEDOLYTIC", "ANTIMICROBIAL", "DEPIGMENTING",
    "BRIGHTENING", "ANTIOXIDANT", "WOUND_HEALING", "ANTI_AGING", "PHOTOPROTECTIVE", "BLEMISH_CARE",
)
# 여드름 결과로 보는 효능(사람 라벨의 효과 방향 비교에 쓴다)
ACNE_EFFECTS = frozenset({"BLEMISH_CARE", "COMEDOLYTIC", "ANTI_INFLAMMATORY", "SEBUM_REGULATION", "KERATOLYTIC"})

_NULLABLE_STRING = {"anyOf": [{"type": "string"}, {"type": "null"}]}

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "relevant": {"type": "boolean"},
        "needs_fulltext": {"type": "boolean"},
        "reason": {"type": "string"},
        "judgments": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "effect_code": {"type": "string", "enum": list(EFFECT_CODES)},
                    "attribution": {"type": "string", "enum": list(ATTRIBUTIONS)},
                    "route": {"type": "string", "enum": list(ROUTES)},
                    "concentration": _NULLABLE_STRING,
                    "population": _NULLABLE_STRING,
                    "study_type": {"type": "string", "enum": list(STUDY_TYPES)},
                    "direction": {"type": "string", "enum": list(DIRECTIONS)},
                    "evidence_quote": {"type": "string"},
                },
                "required": [
                    "effect_code", "attribution", "route", "concentration", "population",
                    "study_type", "direction", "evidence_quote",
                ],
                "additionalProperties": False,
            },
        },
    },
    "required": ["relevant", "needs_fulltext", "reason", "judgments"],
    "additionalProperties": False,
}

PROMPT_VERSION = "evidence-review-v1"

SYSTEM_PROMPT = """You review PubMed records for a cosmetics recommendation graph. For one target ingredient, \
decide which skin effects the record gives evidence about, and how that evidence is attributable to the ingredient.

Return one judgment per effect the record reports an outcome for. Return no judgments when the record has no \
outcome for the target ingredient on skin (for example analytical methods, formulation development without \
skin outcomes, or the ingredient appears only in background sentences).

Fields:
- effect_code: the skin effect the outcome measures.
  - BLEMISH_CARE: overall acne outcomes (acne severity, total lesion count, global grade).
  - COMEDOLYTIC: non-inflammatory lesions or comedones.
  - ANTI_INFLAMMATORY: inflammatory lesions, papules, pustules, or measured inflammation.
  - SEBUM_REGULATION: sebum output or oiliness.
  - Other codes follow their names.
- attribution:
  - single: the target ingredient is the tested active, alone or against a vehicle or another treatment arm.
  - combination: the target is one of several actives in a product, peel solution, or regimen, so its own effect cannot be separated.
  - comparator_only: the target is only the control or comparator arm for another treatment. Report the target arm's own outcome.
- route:
  - topical_leave_on: creams, gels, serums.
  - topical_rinse_off: cleansers, washes.
  - peel: chemical peels, usually 20-30% in office.
  - oral
  - procedure: combined with a device, laser, light therapy, or extraction.
- concentration: as written (for example "2%"), or null.
- population: who or what was studied, briefly (for example "40 adults with moderate acne vulgaris", "mice", "cell line"), or null.
- study_type: rct, cohort, case_series, in_vitro, animal, review, or other.
- direction:
  - improves: the target arm improved the effect.
  - worsens: the target arm worsened the effect.
  - no_difference: no detected change or difference.
  - unclear: the record does not say.
- evidence_quote: copy one sentence or contiguous span from the record verbatim that states the outcome. Do not paraphrase, merge sentences, or fix typos. The quote is checked against the source text by code.

Set needs_fulltext to true when the record suggests a relevant outcome but the abstract is too short to judge it. \
Keep reason to one or two sentences."""


def prompt_sha() -> str:
    payload = json.dumps(
        {"version": PROMPT_VERSION, "system": SYSTEM_PROMPT, "schema": OUTPUT_SCHEMA},
        sort_keys=True, ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def user_message(target_ingredient: str, title: str, source_text: str) -> str:
    return (
        f"Target ingredient: {target_ingredient}\n\n"
        f"<record>\nTitle: {title}\n\n{source_text}\n</record>"
    )
