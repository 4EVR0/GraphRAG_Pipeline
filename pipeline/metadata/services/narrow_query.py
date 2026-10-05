"""성분 × 효능 조합별 좁은 PubMed 검색어 (#49).

상위 N건을 자르는 대신 조합마다 좁게 검색해 결과를 전부 가져오기 위한 쿼리를 만든다.
네트워크 호출은 하지 않는다.
"""
import csv
import re
from dataclasses import dataclass
from pathlib import Path

# 피부와 직접 관련 없는 효능 용어에 붙이는 피부 맥락 조건
SKIN_CONTEXT_TERMS = [
    "skin", "topical", "cutaneous", "dermal", "epidermis", "epidermal",
    "facial", "cosmetic", "dermatolog*",
]
# 몸속 물질·일반 원료에 필수로 붙이는 외용 조건. serum·gel은 혈청·전기영동과 겹쳐 뺀다.
TOPICAL_TERMS = [
    "topical", "topically", "cosmetic*", "cosmeceutical*", "cream", "creams",
    "lotion", "ointment", "emulsion", "skin care", "skincare",
]
BASE_FILTER = "hasabstract AND english[lang]"
# 결과가 많은 조합을 사람 대상 임상·리뷰로 좁히는 조건
NARROW_FILTER = (
    "humans[mh] AND (clinical trial[pt] OR randomized controlled trial[pt] OR review[pt] "
    "OR systematic review[pt] OR meta-analysis[pt])"
)
# 뜻이 여러 개인 짧은 약어(BHA=butylated hydroxyanisole 등)는 동의어에서 뺀다.
_AMBIGUOUS_ABBREVIATION = re.compile(r"[A-Z0-9\-]{2,5}")

RULE_EXCLUDE = "exclude"
RULE_REQUIRE_TOPICAL = "require_topical"
_RULES = frozenset({RULE_EXCLUDE, RULE_REQUIRE_TOPICAL})


@dataclass(frozen=True)
class EffectTerms:
    terms: tuple[str, ...]
    skin_specific: bool


@dataclass(frozen=True)
class PairPlan:
    inci_name: str
    effect_code: str
    query: str | None
    ingredient_terms: tuple[str, ...] = ()
    effect_terms: tuple[str, ...] = ()
    rule: str = ""
    skip_reason: str = ""


def _split(value: str | None) -> list[str]:
    return [part.strip() for part in (value or "").split("|") if part.strip()]


def _read_csv(path: Path) -> list[dict[str, str]]:
    with open(path, encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def load_pairs(path: Path) -> list[dict[str, str]]:
    return _read_csv(path)


def load_effect_terms(path: Path) -> dict[str, EffectTerms]:
    return {
        row["effect_code"].strip(): EffectTerms(
            terms=tuple(_split(row["terms"])),
            skin_specific=row["skin_specific"].strip().lower() == "true",
        )
        for row in _read_csv(path)
    }


def load_ingredient_rules(path: Path) -> dict[str, str]:
    rules = {}
    for row in _read_csv(path):
        rule = row["rule"].strip()
        if rule not in _RULES:
            raise ValueError(f"알 수 없는 성분 규칙: {row['inci_name']}={rule}")
        rules[row["inci_name"].strip().upper()] = rule
    return rules


def load_synonyms(target_csv: Path) -> dict[str, list[str]]:
    """target_ingredients.csv에서 INCI별 검색명과 동의어를 모은다."""
    synonyms: dict[str, list[str]] = {}
    for row in _read_csv(target_csv):
        names = [row.get("query_name", "").strip()] + _split(row.get("alias_list"))
        names = [name for name in names if name]
        for key in {name.upper() for name in names}:
            synonyms.setdefault(key, []).extend(names)
    return synonyms


def _clean(term: str) -> str:
    return term.replace('"', "").strip()


def ingredient_terms(inci_name: str, synonyms: dict[str, list[str]]) -> list[str]:
    """INCI명 + 동의어. INCI명은 항상 쓰고, 동의어 중 모호한 짧은 약어는 뺀다."""
    terms = [_clean(inci_name)]
    for alias in synonyms.get(inci_name.upper(), []):
        alias = _clean(alias)
        if _AMBIGUOUS_ABBREVIATION.fullmatch(alias):
            continue
        terms.append(alias)
    unique: list[str] = []
    for term in terms:
        if term and term.lower() not in {u.lower() for u in unique}:
            unique.append(term)
    return unique


def _words(term: str) -> str:
    return f" {re.sub(r'[^a-z0-9]+', ' ', term.lower()).strip()} "


def drop_self_matching_terms(ing_terms: list[str], effect_terms: tuple[str, ...]) -> list[str]:
    """성분명과 겹치는 효능 용어를 뺀다(MELANIN ↔ "melanin"처럼 성분이 스스로 걸리는 경우)."""
    ing_words = [_words(term) for term in ing_terms]
    kept = []
    for term in effect_terms:
        words = _words(term)
        if any(words in ing or ing in words for ing in ing_words):
            continue
        kept.append(term)
    return kept


def or_clause(terms: list[str] | tuple[str, ...]) -> str:
    return "(" + " OR ".join(f'"{_clean(term)}"[tiab]' for term in terms) + ")"


def build_pair_query(
    ing_terms: list[str],
    effect_terms: list[str],
    skin_specific: bool,
    require_topical: bool = False,
) -> str:
    parts = [or_clause(ing_terms), or_clause(effect_terms), BASE_FILTER]
    if not skin_specific:
        parts.append(or_clause(SKIN_CONTEXT_TERMS))
    if require_topical:
        parts.append(or_clause(TOPICAL_TERMS))
    return " AND ".join(parts)


def narrowed_query(query: str) -> str:
    return f"{query} AND {NARROW_FILTER}"


def plan_pairs(
    pairs: list[dict[str, str]],
    synonyms: dict[str, list[str]],
    effect_terms: dict[str, EffectTerms],
    rules: dict[str, str] | None = None,
) -> list[PairPlan]:
    """조합마다 검색어를 만든다. 검색하지 않는 조합은 skip_reason을 남긴다."""
    rules = rules or {}
    plans = []
    for pair in pairs:
        inci = pair["inci_name"].strip()
        effect = pair["effect_code"].strip()
        rule = rules.get(inci.upper(), "")
        if rule == RULE_EXCLUDE:
            plans.append(PairPlan(inci, effect, None, rule=rule, skip_reason="excluded_ingredient"))
            continue
        spec = effect_terms.get(effect)
        if spec is None:
            plans.append(PairPlan(inci, effect, None, rule=rule, skip_reason="no_effect_terms"))
            continue
        ing = ingredient_terms(inci, synonyms)
        eff = drop_self_matching_terms(ing, spec.terms)
        if not eff:
            plans.append(PairPlan(inci, effect, None, tuple(ing), rule=rule, skip_reason="self_match"))
            continue
        query = build_pair_query(ing, eff, spec.skin_specific, rule == RULE_REQUIRE_TOPICAL)
        plans.append(PairPlan(inci, effect, query, tuple(ing), tuple(eff), rule=rule))
    return plans
