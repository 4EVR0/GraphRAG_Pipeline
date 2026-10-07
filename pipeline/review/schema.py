"""근거 검수 판정 형식과 프롬프트 (#49).

프롬프트·스키마가 바뀌면 prompt_sha가 바뀌고, 같은 PMID도 다시 판정한다.
v2(2026-10-07): 근거 강도 항목(대조군 종류·유의성·대상자 수), route=not_applicable,
효능 정의 보강(미백 두 갈래, 여드름 병변 유형, 장벽·보습 구분, 제품 산화 안정성 제외).
"""
import hashlib
import json
import re

ATTRIBUTIONS = ("single", "combination", "comparator_only")
# not_applicable: 시험관·동물·리뷰처럼 사람이 제품을 쓰는 방식이 없는 경우
ROUTES = ("topical_leave_on", "topical_rinse_off", "peel", "oral", "procedure", "not_applicable")
# #49 시범 범위의 study_type에 동물 연구와 기타(분석법·제형 개발 등)를 더했다.
STUDY_TYPES = ("rct", "cohort", "case_series", "in_vitro", "animal", "review", "other")
DIRECTIONS = ("improves", "worsens", "no_difference", "unclear")
# 대상 성분 쪽 결과를 무엇과 비교했는가
COMPARISONS = ("placebo_or_vehicle", "active_comparator", "untreated_control", "baseline_only", "none", "not_reported")
SIGNIFICANCES = ("significant", "not_significant", "not_reported")
SOURCES = ("abstract", "pmc_fulltext")
EFFECT_CODES = (
    "ANTI_INFLAMMATORY", "SOOTHING", "BARRIER_REPAIR", "HYDRATING", "MOISTURE_RETENTION",
    "SEBUM_REGULATION", "KERATOLYTIC", "COMEDOLYTIC", "ANTIMICROBIAL", "DEPIGMENTING",
    "BRIGHTENING", "ANTIOXIDANT", "WOUND_HEALING", "ANTI_AGING", "PHOTOPROTECTIVE", "BLEMISH_CARE",
)
# 여드름 결과로 보는 효능(사람 라벨의 효과 방향 비교에 쓴다)
ACNE_EFFECTS = frozenset({"BLEMISH_CARE", "COMEDOLYTIC", "ANTI_INFLAMMATORY", "SEBUM_REGULATION", "KERATOLYTIC"})

# 인용 문장에 그 효능의 결과가 실제로 나오는지 대조하는 용어(정규식 조각, 대소문자 무시).
# 한 문장으로 여러 효능을 판정해 효능이 퍼지는 것을 막는다(#50과 같은 유형).
# 약어·임상 척도(AV, GAGS, MASI 등)로만 쓰인 정당한 인용도 있어 사람 검수 큐가 아니라
# 점수 감점 신호로 쓴다(scoring.py).
EFFECT_OUTCOME_TERMS = {
    "ANTI_INFLAMMATORY": r"inflam|papul|pustul|erythem|redness|\bred\b|il-?\d|\bil counts?\b|tnf|cytokine|edema|swelling|nf-?κb|nf-?kb|prostaglandin|cox-?2",
    "SOOTHING": r"sooth|calm|irritat|sting|burn|itch|prurit|discomfort|sensitiv|erythem|redness",
    "BARRIER_REPAIR": r"barrier|tewl|transepidermal|water loss|stratum corneum|lipid|ceramide|integrity|eczema|atopic|dermatitis",
    "HYDRATING": r"hydrat|moistur|water content|corneomet|capacitance|dry|xerosis|tewl|transepidermal",
    "MOISTURE_RETENTION": r"hydrat|moistur|water|humect|retention|corneomet|tewl|dry",
    "SEBUM_REGULATION": r"sebum|sebaceous|sebocyte|lipogen|\bssl\b|greas|oil|seborrh|sebumet|shine|pore",
    "KERATOLYTIC": r"kerat|exfoliat|desquam|dandruff|flak|scal|peel|rough|smooth|texture|corneocyte|comedo|callus|hyperkeratosis",
    "COMEDOLYTIC": r"comed|non-?inflam|\bnil\b|blackhead|whitehead|lesion|acne",
    "ANTIMICROBIAL": r"bacteri|microb|acnes|aureus|fung|candida|malassezia|dermatophyt|tinea|infect|\bmic\b|inhibition zone|kill|colon",
    "DEPIGMENTING": r"pigment|melan|melasma|tyrosinase|lentig|spot|masi|dark|hyperchrom|chloasma|freckle",
    "BRIGHTENING": r"bright|lighten|whiten|tone|lumin|radian|dull|l\*|pigment|melan|spot",
    "ANTIOXIDANT": r"oxida|radical|ros\b|reactive oxygen|scaveng|glutathione|lipid peroxid|malondialdehyde|sod\b",
    "WOUND_HEALING": r"wound|heal|scar|re-?epitheli|closure|ulcer|repair",
    "ANTI_AGING": r"wrinkl|aging|ageing|elastic|firm|collagen|elastin|fine line|photoag|sag|dermal density|thickness",
    "PHOTOPROTECTIVE": r"uv|sun|photo|spf|erythema|sunburn|radiation|light",
    "BLEMISH_CARE": r"acne|\bav\b|lesion|blemish|pimple|breakout|comedo|papul|pustul|gags|\bpga\b|ecca|global (acne )?(assessment|grade)|severity",
}


def effect_in_quote(effect_code: str, quote: str) -> bool:
    pattern = EFFECT_OUTCOME_TERMS.get(effect_code)
    return bool(pattern and re.search(pattern, quote or "", re.IGNORECASE))


_NULLABLE_STRING = {"anyOf": [{"type": "string"}, {"type": "null"}]}
_NULLABLE_INT = {"anyOf": [{"type": "integer"}, {"type": "null"}]}

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
                    "sample_size": _NULLABLE_INT,
                    "study_type": {"type": "string", "enum": list(STUDY_TYPES)},
                    "comparison": {"type": "string", "enum": list(COMPARISONS)},
                    "significance": {"type": "string", "enum": list(SIGNIFICANCES)},
                    "direction": {"type": "string", "enum": list(DIRECTIONS)},
                    "evidence_quote": {"type": "string"},
                },
                "required": [
                    "effect_code", "attribution", "route", "concentration", "population", "sample_size",
                    "study_type", "comparison", "significance", "direction", "evidence_quote",
                ],
                "additionalProperties": False,
            },
        },
    },
    "required": ["relevant", "needs_fulltext", "reason", "judgments"],
    "additionalProperties": False,
}

PROMPT_VERSION = "evidence-review-v2"

SYSTEM_PROMPT = """You review PubMed records for a cosmetics recommendation graph. For one target ingredient, \
decide which skin effects the record gives evidence about, how that evidence is attributable to the ingredient, \
and how strong the comparison behind it is.

Return one judgment per effect the record reports an outcome for. Return no judgments when the record has no \
outcome for the target ingredient on skin, for example analytical methods, formulation development without \
skin outcomes, or the ingredient appearing only in background sentences. Outcomes about the product itself, \
such as preventing a formulation from going rancid or improving its stability, are not skin effects.

Each judgment needs its own evidence_quote that states that effect's outcome. Do not add an effect because \
it is a known property of the ingredient or because another outcome in the sentence implies it.

Fields:
- effect_code: the skin effect the outcome measures.
  - BLEMISH_CARE: overall acne outcomes (acne severity, total lesion count, global grade).
  - COMEDOLYTIC: non-inflammatory lesions or comedones.
  - ANTI_INFLAMMATORY: inflammatory lesions, papules, pustules, or measured inflammation.
  - SEBUM_REGULATION: sebum output or oiliness.
  - KERATOLYTIC: exfoliation, scaling, hyperkeratosis, or skin roughness from excess stratum corneum.
  - DEPIGMENTING: reduced melanin formation or pigmented lesions (melasma, lentigines, post-inflammatory hyperpigmentation, tyrosinase or melanin assays).
  - BRIGHTENING: overall skin tone, lightness, or radiance without a specific pigmented lesion.
  - HYDRATING: skin water content (for example corneometer readings) or dryness.
  - MOISTURE_RETENTION: holding water over time, for example humectant effects after a dry challenge.
  - BARRIER_REPAIR: barrier function, transepidermal water loss, stratum corneum lipids, or barrier-related dermatitis.
  - ANTI_AGING: wrinkles, elasticity, firmness, dermal collagen or elastin, photoaging grades.
  - PHOTOPROTECTIVE: protection against UV, such as SPF, UV-induced erythema, or UV damage markers.
  - Other codes follow their names.
- attribution:
  - single: the target ingredient is the tested active, alone or against a vehicle or another treatment arm.
  - combination: the target is one of several actives in a product, peel solution, or regimen, so its own effect cannot be separated.
  - comparator_only: the target is only the control or comparator arm for another treatment. Report the target arm's own outcome.
- route:
  - topical_leave_on: creams, gels, serums, lotions.
  - topical_rinse_off: cleansers, washes, shampoos.
  - peel: chemical peels, usually 20-30% or higher, applied in a clinic.
  - oral
  - procedure: combined with a device, laser, light therapy, injection, or extraction.
  - not_applicable: in vitro, animal-only without a product route, or reviews.
- concentration: as written (for example "2%" or "2,500 IU/g"), or null.
- population: who or what was studied, briefly (for example "40 adults with moderate acne vulgaris", "mice", "cell line"), or null.
- sample_size: number of human participants or animals analysed, or null if not stated or in vitro.
- study_type: rct, cohort, case_series, in_vitro, animal, review, or other.
- comparison: what the target arm's result was compared with.
  - placebo_or_vehicle: placebo, vehicle, or the same base without the target.
  - active_comparator: another active treatment.
  - untreated_control: an untreated area or group.
  - baseline_only: before versus after in the same people, with no control.
  - none: no comparison (for example a review statement or a single measurement).
  - not_reported: the record does not say.
- significance: significant if the record reports a statistically significant result for this outcome, not_significant if it reports no significant difference, otherwise not_reported.
- direction:
  - improves: the target arm improved the effect.
  - worsens: the target arm worsened the effect.
  - no_difference: no detected change or difference.
  - unclear: the record does not say.
- evidence_quote: copy one sentence or contiguous span from the record verbatim that states the outcome. Do not paraphrase, merge sentences, or fix typos. The quote is checked against the source text by code.

Set needs_fulltext to true when the record suggests a relevant outcome but the abstract is too short to judge it, \
for example when results for several conditions are pooled. Keep reason to one or two sentences."""


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
