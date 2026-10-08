"""검수 판정의 연구 대상 질환 분류와 고민별 근거 점수 (#49).

효능 엣지는 질환을 구분하지 않아, 예를 들어 요소의 각질 제거 근거(건조증·발·어린선 연구)가
여드름 고민 순위에 들어갔다. 판정마다 연구 대상(population)과 제목으로 질환을 규칙 분류하고,
고민마다 맞는 질환의 사람 대상 연구만 세어 (성분, 고민) 점수를 만든다. LLM 호출은 없다.

민감 피부 계열 고민은 '민감함을 고치는 성분'이 아니라 '민감한 피부에 써도 되는 성분'을 뜻한다.
그래서 민감·아토피 피부에 실제로 바른 연구만 센다(건강한 피부 연구 제외). 자극 우려 성분 목록
(config/review/sensitive_skin_cautions.csv)은 조치 강도를 나눈다.
- exclude: 직접 근거나 표시 의무가 있는 성분. 근거가 있어도 엣지를 만들지 않는다.
- caution: 계열·구성 성분으로 추정한 성분. 엣지는 두고 caution 표시만 단다.
목록에 없다고 '민감 피부에 안전함' 점수를 주지는 않는다.
caution_with_concerns(예: 각질 제거 산의 여드름 계열)는 요청 시점 규칙이라 여기서는 쓰지 않고,
그래프 노드(sensitive_caution_with)로 서버에 넘긴다. 민감 계열 고민의 근거 엣지에서는 계속 뺀다.
"""
import csv
import math
import re
from collections import defaultdict
from pathlib import Path

CONTEXT_RULES_VERSION = "condition-context-v4"

# 질환 묶음. 한 판정이 여러 묶음에 들 수 있다(예: 아토피 환자의 건조증).
CONDITION_PATTERNS = {
    "acne": r"\bacne|comedo|acneiform|pimple|blemish",
    "oily": r"\boily|seborrh(o)?ea\b|sebum|greas|enlarged pore|large pore",
    "pigment": r"melasma|chloasma|hyperpigment|pigmented|pigmentation|lentig|freckle|dark spot|\bpih\b|"
               r"post-?inflammatory hyper|dark circle|skin tone|uneven tone",
    "aging": r"\baging|ageing|photoag|photodamage|wrinkle|fine line|nasolabial|crow'?s feet|elasticity|"
             r"sagging|laxity|mature skin|postmenopausal|elderly|older (wom|adult|subject)",
    "photo": r"\buv[ab]?\b|ultraviolet|sunburn|solar simulat|sun[- ]exposed|photoprotect|\bspf\b|\bmed\b",
    "dry": r"xerosis|dry skin|dryness|\bdry\b|dehydrat|ichthyosis|flak|scaling|rough skin",
    # 아토피·습진만. 기저귀·손·접촉 피부염 같은 자극성 피부염은 irritation으로 보낸다.
    "atopic": r"atopic|eczema",
    # 민감 피부로 모집한 대상. 'sensitive, mild ... skin'처럼 쉼표로 이어진 표현도 잡는다.
    "sensitive": r"sensitive[- ]?skin|\bsensitive,|skin sensitivity|allergy-prone|reactive skin|stinging|rosacea|"
                 r"couperose|telangiect",
    # 자극을 일부러 준 피부나 홍반(자외선 홍반 포함). 민감 피부 자체와는 구분한다.
    "irritation": r"irritat|erythema|redness|"
                  r"(diaper|napkin|incontinence[- ]associated|hand|occupational|contact) dermatitis",
    "keratinization": r"keratosis pilaris|hyperkeratosis|callus|calluses|\bheels?\b|\bfeet\b|\bfoot\b|plantar|keratoderma",
    "scar": r"\bscar|atrophic|wound|ulcer|post-?(laser|procedure|operative)|surgical",
    "other_disease": r"psoriasis|actinic keratos|alopecia|tinea|onychomycosis|vitiligo|diabetic|cancer|"
                     r"chemotherapy|radiation|lupus|scabies|seborrh(e|o)ic dermatitis|herpes|leishmania",
    "healthy": r"healthy|normal skin|volunteer",
}
HUMAN_STUDIES = frozenset({"rct", "cohort", "case_series", "review"})
# 연구 유형이 사람 대상이어도 대상이 동물이면 사람 연구로 세지 않는다(예: 소 발굽 피부염 코호트).
ANIMAL_POPULATION = r"\b(bovine|canine|feline|equine|porcine|cows?|cattle|dogs?|cats?|horses?|pigs?|mice|rats?)\b"


def classify_conditions(record: dict, title: str = "") -> set[str]:
    """판정의 연구 대상 질환 묶음. 사람 대상이 아니면 {'nonhuman'}, 단서가 없으면 {'unspecified'}."""
    if record.get("study_type") not in HUMAN_STUDIES:
        return {"nonhuman"}
    if re.search(ANIMAL_POPULATION, (record.get("population") or "").lower()):
        return {"nonhuman"}
    text = f"{record.get('population') or ''} {title or ''}".lower()
    found = {name for name, pattern in CONDITION_PATTERNS.items() if re.search(pattern, text)}
    return found or {"unspecified"}


def load_concern_conditions(path: Path) -> dict[str, tuple[frozenset[str], frozenset[str], bool]]:
    """concern_code → (효능 코드 집합, 인정 질환 묶음 집합, 자극 우려 성분 제외 여부)."""
    table = {}
    with open(path, encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            effects = frozenset(e for e in row["effect_codes"].split("|") if e)
            conditions = frozenset(c for c in row["conditions"].split("|") if c)
            unknown = conditions - set(CONDITION_PATTERNS)
            if unknown:
                raise ValueError(f"{row['concern_code']}: 알 수 없는 질환 묶음 {sorted(unknown)}")
            table[row["concern_code"]] = (effects, conditions, row.get("sensitive_caution") == "yes")
    return table


CAUTION_ACTIONS = frozenset({"exclude", "caution"})


def load_sensitive_cautions(path: Path) -> dict[str, dict[str, str]]:
    """민감 피부 계열 고민의 자극 우려 성분 INCI → 행(caution_class, action, condition, evidence_scope, ...)."""
    table = {}
    with open(path, encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["action"] not in CAUTION_ACTIONS:
                raise ValueError(f"{row['inci_name']}: 알 수 없는 조치 {row['action']!r}")
            table[row["inci_name"].strip().upper()] = row
    return table


def _caution_label(row: dict[str, str] | None) -> str:
    return f"{row['caution_class']}:{row['evidence_scope']}" if row and row["action"] == "caution" else ""


def score_concerns(
    scored: list[dict],
    concern_table: dict[str, tuple[frozenset[str], frozenset[str], bool]],
    titles: dict[str, str] | None = None,
    cautions: dict[str, dict[str, str]] | None = None,
) -> tuple[list[dict], list[dict]]:
    """판정별 점수(scoring.score_records 출력) → (판정에 질환 표시를 더한 행, (성분, 고민) 근거 행).

    고민마다 그 고민의 효능이면서, 사람 대상 연구이고, 인정 질환 묶음에 드는 판정만 센다.
    같은 논문은 한 번만, 가장 큰 가중치로 센다. 민감 계열 고민에서 exclude 성분은 빼고,
    caution 성분은 엣지에 caution(분류:근거 범위)을 단다.
    """
    titles = titles or {}
    cautions = cautions or {}
    marked = []
    by_edge: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    effects_used: dict[tuple[str, str], set[str]] = defaultdict(set)
    for record in scored:
        conditions = classify_conditions(record, titles.get(str(record["pmid"]), ""))
        marked.append({**record, "conditions": "|".join(sorted(conditions))})
        if float(record.get("weight") or 0) <= 0 or "nonhuman" in conditions:
            continue
        inci = (record.get("ingredient_inci") or "").upper()
        for concern, (effects, allowed, exclude_cautions) in concern_table.items():
            if exclude_cautions and cautions.get(inci, {}).get("action") == "exclude":
                continue
            if record.get("effect_code") in effects and conditions & allowed:
                key = (inci, concern)
                pmid = str(record["pmid"])
                by_edge[key][pmid] = max(by_edge[key].get(pmid, 0.0), float(record["weight"]))
                effects_used[key].add(record["effect_code"])
    edges = [
        {"ingredient_inci": inci, "concern_code": concern,
         "score": round(math.log1p(sum(papers.values())), 6), "paper_count": len(papers),
         "effects": "|".join(sorted(effects_used[(inci, concern)])),
         "top_pmids": "|".join(sorted(papers, key=papers.get, reverse=True)[:5]),
         "caution": _caution_label(cautions.get(inci)) if concern_table[concern][2] else "",
         "rules_version": CONTEXT_RULES_VERSION}
        for (inci, concern), papers in by_edge.items()
    ]
    edges.sort(key=lambda e: (e["concern_code"], -e["score"], e["ingredient_inci"]))
    return marked, edges
