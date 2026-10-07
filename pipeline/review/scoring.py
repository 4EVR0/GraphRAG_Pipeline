"""검수 판정 → (성분, 효능) 근거 점수 (#49). 점수는 LLM이 아니라 이 규칙이 계산한다.

판정 하나의 가중치 = 연구 유형 × 성분 귀속 × 대조군 × 유의성 × 감점.
- 제품 추천 근거가 아닌 것은 0: 필링·시술·경구, 개선이 아닌 결과, 유의하지 않은 결과,
  원문 대조 실패·허용값 오류.
- 감점: 인용 문장에 그 효능 결과가 없음(effect_in_quote=False), 제형 역할 성분, 소규모 사람 연구.
(성분, 효능) 점수 = log1p(논문별 최대 가중치의 합). 같은 논문은 한 번만 센다(#50과 같은 방식).

v1 판정(대조군·유의성·대상자 수 없음)은 not_reported로 본다.
"""
import csv
import math
from collections import defaultdict
from pathlib import Path

RULES_VERSION = "review-scoring-v1"

STUDY_WEIGHT = {"rct": 1.0, "cohort": 0.6, "review": 0.5, "case_series": 0.35,
                "animal": 0.15, "in_vitro": 0.1, "other": 0.1}
ATTRIBUTION_WEIGHT = {"single": 1.0, "comparator_only": 0.6, "combination": 0.35}
COMPARISON_WEIGHT = {"placebo_or_vehicle": 1.0, "active_comparator": 0.8, "untreated_control": 0.8,
                     "baseline_only": 0.6, "not_reported": 0.6, "none": 0.5}
SIGNIFICANCE_WEIGHT = {"significant": 1.0, "not_reported": 0.7, "not_significant": 0.0}
# 화장품 제품 추천 근거로 쓰지 않는 적용 형태
NON_PRODUCT_ROUTES = frozenset({"peel", "procedure", "oral"})
HUMAN_STUDIES = frozenset({"rct", "cohort", "case_series"})
SMALL_HUMAN_STUDY = 20
EFFECT_NOT_IN_QUOTE_FACTOR = 0.5
FORMULATION_ROLE_FACTOR = 0.5
SMALL_STUDY_FACTOR = 0.8

# CosIng 용도 중 제품을 만드는 데 쓰는 용도와 피부 효능에 해당하는 용도.
# 일반 SKIN CONDITIONING·HAIR CONDITIONING·FRAGRANCE는 어느 쪽 근거로도 보지 않는다.
FORMULATION_FUNCTIONS = frozenset({
    "SURFACTANT", "SURFACTANT - CLEANSING", "SURFACTANT - EMULSIFYING", "SURFACTANT - FOAM BOOSTING",
    "SURFACTANT - HYDROTROPE", "SURFACTANT - SOLUBILIZING", "SURFACTANT - DISPERSING",
    "VISCOSITY CONTROLLING", "CLEANSING", "ANTISTATIC", "FILM FORMING", "EMULSION STABILISING", "SOLVENT",
    "BINDING", "FOAMING", "OPACIFYING", "HAIR FIXING", "PRESERVATIVE", "BUFFERING", "BULKING", "CHELATING",
    "ABSORBENT", "LIGHT STABILIZER", "PLASTICISER", "ANTICAKING", "COLORANT", "ANTIFOAMING", "DENATURANT",
    "GEL FORMING", "ANTICORROSIVE", "PROPELLANT", "DISPERSING NON-SURFACTANT", "SLIP MODIFIER",
    "PH ADJUSTERS", "SURFACE MODIFIER", "ADHESIVE", "PEARLESCENT",
})
BENEFIT_FUNCTIONS = frozenset({
    "HUMECTANT", "SKIN CONDITIONING - EMOLLIENT", "SKIN CONDITIONING - HUMECTANT", "SKIN CONDITIONING - OCCLUSIVE",
    "SKIN PROTECTING", "ANTIOXIDANT", "ANTIMICROBIAL", "ASTRINGENT", "TONIC", "BLEACHING", "UV ABSORBER",
    "UV FILTER", "ANTI-SEBUM", "ANTI-SEBORRHEIC", "SOOTHING", "EXFOLIATING", "KERATOLYTIC", "MOISTURISING",
    "REFRESHING", "SMOOTHING", "DEODORANT", "ANTIPERSPIRANT", "TANNING", "REFATTING", "ABRASIVE",
})


def load_cosing_functions(path: Path) -> dict[str, set[str]]:
    """KCIA↔CosIng Gold CSV(inci_name, cosing_functions)에서 INCI별 CosIng 용도를 읽는다."""
    functions: dict[str, set[str]] = defaultdict(set)
    with open(path, encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            values = {f.strip().upper() for f in (row.get("cosing_functions") or "").split(";") if f.strip()}
            functions[(row.get("inci_name") or "").strip().upper()] |= values
    return dict(functions)


def formulation_role(functions: set[str] | None) -> bool:
    """제형 용도만 있고 구체적 피부 효능 용도가 없는 성분(점증제·용제 등)."""
    if not functions:
        return False
    return bool(functions & FORMULATION_FUNCTIONS) and not (functions & BENEFIT_FUNCTIONS)


def load_mfds_functional(path: Path) -> dict[tuple[str, str], dict]:
    """식약처 기능성 고시 원료 CSV → (INCI, 효능 코드) → {function, max_content, condition}."""
    table = {}
    with open(path, encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            inci = (row.get("inci_name") or "").strip().upper()
            if not inci:
                continue
            for effect in (row.get("effect_codes") or "").split("|"):
                if effect.strip():
                    table[(inci, effect.strip())] = {k: row.get(k, "") for k in ("function", "max_content", "condition")}
    return table


def judgment_weight(record: dict, functions: set[str] | None = None) -> tuple[float, list[str]]:
    """판정 하나의 가중치와 사유. 사유는 0이 된 이유와 감점 이유를 모두 담는다."""
    reasons = []
    if not record.get("quote_verified") or not record.get("values_valid", True):
        return 0.0, ["unverified"]
    if record.get("direction") != "improves":
        return 0.0, [f"direction={record.get('direction')}"]
    if record.get("route") in NON_PRODUCT_ROUTES:
        return 0.0, [f"route={record.get('route')}"]
    significance = record.get("significance") or "not_reported"
    if SIGNIFICANCE_WEIGHT.get(significance, 0.7) == 0.0:
        return 0.0, ["not_significant"]
    weight = (
        STUDY_WEIGHT.get(record.get("study_type"), 0.1)
        * ATTRIBUTION_WEIGHT.get(record.get("attribution"), 0.35)
        * COMPARISON_WEIGHT.get(record.get("comparison") or "not_reported", 0.6)
        * SIGNIFICANCE_WEIGHT.get(significance, 0.7)
    )
    if record.get("effect_in_quote") is False:
        weight *= EFFECT_NOT_IN_QUOTE_FACTOR
        reasons.append("effect_not_in_quote")
    if formulation_role(functions):
        weight *= FORMULATION_ROLE_FACTOR
        reasons.append("formulation_role")
    size = record.get("sample_size")
    if record.get("study_type") in HUMAN_STUDIES and isinstance(size, int) and size < SMALL_HUMAN_STUDY:
        weight *= SMALL_STUDY_FACTOR
        reasons.append("small_study")
    return round(weight, 6), reasons


def score_records(
    records: list[dict],
    cosing: dict[str, set[str]] | None = None,
    mfds: dict[tuple[str, str], dict] | None = None,
) -> tuple[list[dict], list[dict]]:
    """판정 레코드 → (판정별 점수 행, (성분, 효능) 집계 행)."""
    cosing = cosing or {}
    mfds = mfds or {}
    scored = []
    by_edge: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    human: dict[tuple[str, str], set[str]] = defaultdict(set)
    for record in records:
        inci = (record.get("ingredient_inci") or "").upper()
        effect = record.get("effect_code") or ""
        weight, reasons = judgment_weight(record, cosing.get(inci))
        functional = mfds.get((inci, effect))
        flags = []
        if functional:
            flags.append(f"mfds_{functional['function']}")
            # 여드름 기능성 고시는 씻어내는 제품에만 해당한다.
            if functional["function"] == "acne" and record.get("route") == "topical_leave_on":
                flags.append("mfds_route_mismatch")
        scored.append({**record, "weight": weight, "weight_reasons": "|".join(reasons),
                       "flags": "|".join(flags), "rules_version": RULES_VERSION})
        if weight > 0:
            key = (inci, effect)
            pmid = str(record["pmid"])
            by_edge[key][pmid] = max(by_edge[key].get(pmid, 0.0), weight)
            if record.get("study_type") in HUMAN_STUDIES:
                human[key].add(pmid)
    edges = [
        {"ingredient_inci": inci, "effect_code": effect,
         "score": round(math.log1p(sum(papers.values())), 6), "paper_count": len(papers),
         "human_paper_count": len(human[(inci, effect)]),
         "top_pmids": "|".join(sorted(papers, key=papers.get, reverse=True)[:5]),
         "mfds_functional": bool(mfds.get((inci, effect))), "rules_version": RULES_VERSION}
        for (inci, effect), papers in by_edge.items()
    ]
    edges.sort(key=lambda e: (-e["score"], e["ingredient_inci"], e["effect_code"]))
    return scored, edges
