"""
Gold evidence / canonical scoring (graph_goldlayer_design_v3 + v4):
v4: richer ingredient detection, procedure_adjunct vs post_procedure_recovery_formulation,
list-pattern strict guard, strict requires single detected unit.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple

_CANONICAL_RELATION_MAP: Dict[str, str] = {
    "is_safe_for": "is_well_tolerated_for",
    "increases": "improves",
    "inhibits": "reduces",
    "stimulates": "improves",
    "regulates": "improves",
    "modulates": "improves",
}

_STRENGTH_WEIGHT = {"strong": 1.0, "moderate": 0.75, "weak": 0.35}
_SIGNIFICANCE_WEIGHT = {
    "significant": 1.0,
    "unclear": 0.8,
    "not_applicable": 0.7,
    "not_significant": 0.05,
}
_ATTRIBUTION_WEIGHT = {
    "single_active": 1.0,
    "single_formulation": 0.8,
    "multi_active_combination": 0.45,
    "procedure_adjunct_combination": 0.35,
    "post_procedure_recovery_formulation": 0.72,
    "procedure_combination": 0.35,
    "ambiguous": 0.2,
}

_STUDY_CONTEXT_WEIGHT = {
    "human_topical": 1.0,
    "human_oral": 0.95,
    "human_intradermal": 0.9,
    "in_vitro": 0.4,
    "animal": 0.65,
    "review": 0.5,
    "unknown": 0.75,
}

_TIER_RANK = {
    "strict_graph": 3,
    "soft_graph": 2,
    "recommendation_only": 1,
    "evidence_only": 0,
}

_GRAPH_TIERS = frozenset({"strict_graph", "soft_graph"})
_RECO_TIERS = frozenset({"strict_graph", "soft_graph", "recommendation_only"})
_VALID_TIERS = frozenset(_TIER_RANK.keys())

# v4 §2: adjunct (co-therapy) vs post-procedure recovery + formulation
_ADJUNCT_PROCEDURE_RE = re.compile(
    r"microneedling|micro[\s-]?needling|"
    r"combined\s+with\s+(?:a\s+)?(?:fractional\s+)?laser|"
    r"with\s+microneedling|microneedling\s+with|"
    r"adjunctive\s+.*(?:laser|microneedling)|"
    r"(?:laser|microneedling)\s+with\s+(?:tranexamic|vitamin|tca|acid)|"
    r"plus\s+(?:fractional\s+)?laser",
    re.IGNORECASE,
)

_POST_PROCEDURE_RECOVERY_RE = re.compile(
    r"after\s+(?:non[\s-]?ablative\s+)?laser|after\s+facial\s+laser|"
    r"post[\s-]?(?:non[\s-]?ablative\s+)?laser|post[\s-]?procedure|"
    r"following\s+laser|recovery\s+after\s+laser|laser[\s-]?(?:induced|treatment)|"
    r"post\s+(?:non[\s-]?ablative\s+)?laser\s+",
    re.IGNORECASE,
)

# Broad procedure / device (fallback when not recovery+formulation)
_BROAD_PROCEDURE_RE = re.compile(
    r"\blaser\b|fractional\s+laser|\bpeeling\b|radiofrequency|\bprocedure\b|"
    r"\blllt\b|\bipl\b|phototherapy|subcision|chemical\s+peel|cryotherapy|"
    r"laser\s+treatment|laser\s+irradiation",
    re.IGNORECASE,
)

# v4 §4: list-style multi-ingredient → never strict (regex safety net)
_LIST_PATTERN_STRICT_BLOCK_RE = re.compile(
    r"\bsuch\s+as\s+[^.]{0,200},[^.]{0,200},|"
    r"\bincluding\s+[^.]{0,200},[^.]{0,200},|"
    r"\balong\s+with\s+[^.]{0,200},|"
    r"\bcontaining\s+[^.]{0,200},\s*[^.]{0,80},\s*(?:and|&)\s+|"
    r"\bsupplemented\s+with\s+[^.]{0,120}\s+and\s+|"
    r",\s*[^,]{3,50},\s*and\s+[a-z]|"
    r"\bwith\s+soothing\s+ingredients\s+such\s+as\b",
    re.IGNORECASE,
)

# v3 §2-3 formulation (product/delivery)
_FORMULATION_RE = re.compile(
    r"\benriched\b|\bcontaining\b|\binfused\b|\bointment\b|\bcream\b|\blotion\b|"
    r"\bmoisturizer\b|\bemollient\b|\bserum\b|\bmask\b|\bgel\b|\bhydrogel\b|\bpatch\b|"
    r"topical\s+formulation|\bformulation\b|\bvehicle\b|-enriched|-containing",
    re.IGNORECASE,
)

_PSEUDO_ACTIVE_PATTERNS: List[Tuple[str, str]] = [
    (r"\bvitamin\s+c\b", "vit_c"),
    (r"\bascorbic\s+acid\b", "vit_c"),
    (r"\bglabridin\b", "glabridin"),
    (r"\bellagic\s+acid\b", "ellagic"),
    (r"\bpolypodium\b", "ple"),
    (r"\bhydroquinone\b", "hq"),
    (r"\btretinoin\b", "tret"),
    (r"\bglycolic\s+acid\b", "glycolic"),
    (r"\bsalicylic\s+acid\b", "sal_acid"),
    (r"\btrichloroacetic\b", "tca"),
    (r"\b15%\s*tca\b", "tca"),
    (r"\btca\s+chemical\b", "tca"),
    (r"\bhyaluronic\s+acid\b", "ha"),
]

# v4 §1: common INCI / botanical lines not always in target CSV
_EXTRA_INCI_PATTERNS: List[Tuple[str, str]] = [
    (r"\bmadecassoside\b", "madecassoside"),
    (r"\bdipotassium\s+glycyrrhizate\b", "dipotassium_glycyrrhizate"),
    (r"\bportulaca\s+oleracea\b", "portulaca_oleracea"),
    (r"\bplatinum\s+liposome\b", "platinum_liposome"),
    (r"\bliposome\s+technology\b", "liposome_technology"),
    (r"\bcentella\b", "centella"),
    (r"\bceramide\s+np\b", "ceramide_np"),
]

_LIST_INTRO_RE = re.compile(
    r"(?:such\s+as|including|along\s+with|supplemented\s+with|containing)\s+"
    r"(.{3,400}?)(?=\.|;| and \w+ \w+ \w+| compared| showed| demonstrate|$)",
    re.IGNORECASE | re.DOTALL,
)

_VERB_HINTS = (
    "improv",
    "reduc",
    "show",
    "demonstr",
    "conclusion",
    "result",
    "evaluat",
    "study",
    "method",
    "patient",
    "significant",
    "objective",
    "background",
)


def slug_token(text: str) -> str:
    t = (text or "").lower().strip()
    t = re.sub(r"[^a-z0-9]+", "_", t)
    return t.strip("_") or "unknown"


def normalize_relation_for_canonical(relation: str) -> str:
    r = (relation or "").strip().lower()
    preferred = {"improves", "reduces", "prevents", "is_well_tolerated_for"}
    if r in preferred:
        return r
    return _CANONICAL_RELATION_MAP.get(r, r)


def build_canonical_claim_key(
    ingredient_name: str,
    relation: str,
    target: str,
    target_category: str,
) -> str:
    rel_norm = normalize_relation_for_canonical(relation)
    return "|".join(
        (
            slug_token(ingredient_name),
            slug_token(rel_norm),
            slug_token(target),
            slug_token(target_category),
        )
    )


def build_dedup_scope_key(
    pmid: str,
    source_sentence: str,
    ingredient_name: str,
    relation: str,
    target: str,
) -> str:
    raw = "|".join(
        (
            (pmid or "").strip(),
            (source_sentence or "").strip(),
            (ingredient_name or "").strip().lower(),
            (relation or "").strip().lower(),
            (target or "").strip().lower(),
        )
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def build_evidence_id(batch_id: str, dedup_scope_key: str) -> str:
    raw = f"{batch_id}|{dedup_scope_key}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _ingredient_in_sentence(canonical_ingredient: str, sentence: str) -> bool:
    lower = sentence.lower()
    ing = canonical_ingredient.strip().lower()
    if ing and re.search(r"\b" + re.escape(ing) + r"\b", lower):
        return True
    if ing == "tranexamic acid" and (
        re.search(r"\btxa\b", lower) or "tranexamic" in lower
    ):
        return True
    if ing == "panthenol" and "dexpanthenol" in lower:
        return True
    if ing == "hyaluronic acid" and (
        "hyaluron" in lower or "sodium hyaluronate" in lower
    ):
        return True
    if ing == "ascorbic acid" and (
        re.search(r"\b(?:l-)?ascorbic\s+acid\b", lower)
        or "vitamin c" in lower
        or re.search(r"\bvit\s+c\b", lower)
    ):
        return True
    if ing == "zinc pca" and re.search(r"\bzinc\s+pca\b", lower):
        return True
    if ing == "centella asiatica" and (
        re.search(r"\bcentella\b", lower) or "gotu kola" in lower
    ):
        return True
    if ing == "madecassoside" and "madecassoside" in lower:
        return True
    if ing == "retinol" and re.search(r"\bretinol\b", lower):
        return True
    if ing == "coenzyme q10" and (
        "ubiquinone" in lower or "coq10" in lower or "coenzyme q10" in lower
    ):
        return True
    return False


def has_strict_blocking_list_pattern(sentence: str) -> bool:
    """v4 §4: multi-ingredient list phrasing → strict forbidden."""
    return bool(sentence and _LIST_PATTERN_STRICT_BLOCK_RE.search(sentence))


def count_distinct_detection_units(labels: Sequence[str]) -> int:
    return len({x.strip() for x in labels if x and str(x).strip()})


def ingredient_detection_suspect(sentence: str, labels: Sequence[str]) -> bool:
    """v4 §3: list pattern visible but detector still shows a single unit."""
    return has_strict_blocking_list_pattern(sentence) and count_distinct_detection_units(labels) <= 1


def _labels_from_newlines(sentence: str) -> List[str]:
    """v4: newline-separated INCI / tech lines (e.g. platinum liposome / panthenol / …)."""
    if not sentence or "\n" not in sentence:
        return []
    parts = [p.strip() for p in sentence.splitlines() if p.strip()]
    if len(parts) < 2:
        return []
    out: List[str] = []
    seen: Set[str] = set()
    for p in parts:
        pl = p.lower()
        if any(v in pl for v in _VERB_HINTS):
            continue
        if len(pl) < 3 or len(pl) > 120:
            continue
        key = f"[line:{pl[:100]}]"
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out if len(out) >= 2 else []


def _labels_from_list_intros(sentence: str, allowed_canonicals: Sequence[str]) -> List[str]:
    """v4 §1: such as / including / along with / containing … comma lists."""
    if not sentence:
        return []
    found: List[str] = []
    seen: Set[str] = set()
    lower_full = sentence.lower()
    for m in _LIST_INTRO_RE.finditer(sentence):
        span = m.group(1)
        sl = span.lower()
        for c in sorted(allowed_canonicals, key=len, reverse=True):
            cl = c.strip().lower()
            if cl and re.search(r"\b" + re.escape(cl) + r"\b", sl):
                if c not in seen:
                    seen.add(c)
                    found.append(c)
        for pattern, tag in _PSEUDO_ACTIVE_PATTERNS + _EXTRA_INCI_PATTERNS:
            if re.search(pattern, sl, re.IGNORECASE) and tag not in seen:
                seen.add(tag)
                found.append(f"[{tag}]")
        if sl.count(",") >= 1 and len(found) >= 1:
            for c in sorted(allowed_canonicals, key=len, reverse=True):
                cl = c.strip().lower()
                if cl and cl in sl and c not in seen:
                    seen.add(c)
                    found.append(c)
    if re.search(r"\bcontaining\s+[^.]{0,120},\s*[^.]{0,120}\s+and\s+", lower_full):
        for pattern, tag in _EXTRA_INCI_PATTERNS:
            if re.search(pattern, lower_full, re.IGNORECASE) and tag not in seen:
                seen.add(tag)
                found.append(f"[{tag}]")
    return found


def list_detected_ingredients_in_sentence(
    sentence: str,
    allowed_canonicals: Sequence[str],
) -> List[str]:
    """Canonical + pseudo + INCI + newline/list-intro expansion (v4 audit + strict count)."""
    lower = (sentence or "").lower()
    labels: List[str] = []
    seen: Set[str] = set()
    for c in sorted(allowed_canonicals, key=len, reverse=True):
        cl = c.strip().lower()
        if cl and re.search(r"\b" + re.escape(cl) + r"\b", lower):
            if c not in seen:
                seen.add(c)
                labels.append(c)
    if re.search(r"\btxa\b", lower) or "tranexamic" in lower:
        for c in allowed_canonicals:
            if "tranexamic" in c.lower() and c not in seen:
                seen.add(c)
                labels.append(c)
                break
    if "dexpanthenol" in lower:
        for c in allowed_canonicals:
            if "panthenol" in c.lower() and c not in seen:
                seen.add(c)
                labels.append(c)
                break
    for pattern, tag in _PSEUDO_ACTIVE_PATTERNS:
        if re.search(pattern, lower, re.IGNORECASE):
            lab = f"[{tag}]"
            if lab not in seen:
                seen.add(lab)
                labels.append(lab)
    for pattern, tag in _EXTRA_INCI_PATTERNS:
        if re.search(pattern, lower, re.IGNORECASE):
            lab = f"[{tag}]"
            if lab not in seen:
                seen.add(lab)
                labels.append(lab)
    for extra in _labels_from_list_intros(sentence, allowed_canonicals):
        if extra not in seen:
            seen.add(extra)
            labels.append(extra)
    for extra in _labels_from_newlines(sentence):
        if extra not in seen:
            seen.add(extra)
            labels.append(extra)
    return labels


def _detection_labels_to_tags(labels: Sequence[str]) -> Set[str]:
    tags: Set[str] = set()
    for lab in labels:
        s = str(lab).strip()
        if s.startswith("[line:"):
            tags.add(f"l:{s}")
        elif s.startswith("[") and s.endswith("]"):
            tags.add("p:" + s[1:-1])
        else:
            tags.add("c:" + s)
    return tags


def _ingredient_tags_in_clause(clause: str, allowed_canonicals: Sequence[str]) -> Set[str]:
    """Clause-level units aligned with list_detected (v4)."""
    return _detection_labels_to_tags(
        list_detected_ingredients_in_sentence(clause, allowed_canonicals)
    )


def _primary_clause(sentence: str, ingredient_name: str) -> str:
    s = (sentence or "").strip()
    if not s:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", s)
    for cl in parts:
        if _ingredient_in_sentence(ingredient_name, cl):
            return cl.strip()
    return s


def _efficacy_predicates_near(text: str, start: int, end: int, window: int = 80) -> bool:
    chunk = text[max(0, start - window) : min(len(text), end + window)].lower()
    return any(
        p in chunk
        for p in (
            "improv",
            "reduc",
            "enhanc",
            "promot",
            "effect",
            "efficac",
            "demonstrat",
            "show",
            "treat",
            "decreas",
            "superior",
        )
    )


def _clause_suggests_multi_actives(clause: str, tags: Set[str]) -> bool:
    """v3 §3 + v4 newline list lines."""
    line_tags = [t for t in tags if t.startswith("l:")]
    if len(line_tags) >= 2:
        return True
    if len(tags) < 2:
        return False
    lower = clause.lower()
    if re.search(
        r"combined\s+with|supplemented\s+with|,\s*[^,]{2,50},\s+and\s+|"
        r"\bplus\b|with\s+vitamin\s+c\b|containing\s+[^.]{0,80},\s*[^.]{0,80}\s+and\s+",
        lower,
    ):
        return True
    if " and " in lower:
        return True
    tokens = lower.split()
    positions: List[int] = []
    for i, tok in enumerate(tokens):
        for t in tags:
            key = t[2:] if t.startswith(("c:", "p:")) else t
            if len(key) > 2 and key.lower() in tok:
                positions.append(i)
                break
    if len(positions) >= 2:
        span = max(positions) - min(positions)
        if span <= 12:
            lo = min(positions)
            hi = max(positions)
            approx_start = sum(len(tokens[j]) + 1 for j in range(lo))
            approx_end = approx_start + 40
            if _efficacy_predicates_near(clause, approx_start, approx_end):
                return True
            if span <= 6:
                return True
    return False


def _adjunct_procedure_hit(sentence: str) -> bool:
    return bool(sentence and _ADJUNCT_PROCEDURE_RE.search(sentence))


def _post_procedure_recovery_hit(sentence: str) -> bool:
    return bool(sentence and _POST_PROCEDURE_RECOVERY_RE.search(sentence))


def _formulation_hit(sentence: str) -> bool:
    return _FORMULATION_RE.search(sentence or "") is not None


def reconcile_attribution_v4(
    sentence: str,
    attribution: str,
    detected_labels: Sequence[str],
) -> str:
    """v4: list-pattern / multi-line → never leave as single_active when structure says multi."""
    s = sentence or ""
    n = count_distinct_detection_units(detected_labels)
    if n >= 2 and attribution == "single_active":
        return "multi_active_combination"
    if has_strict_blocking_list_pattern(s) and attribution == "single_active":
        return "multi_active_combination"
    if len(_labels_from_newlines(s)) >= 2 and attribution == "single_active":
        return "multi_active_combination"
    return attribution


def label_attribution_v2(
    sentence: str,
    ingredient_name: str,
    allowed_canonicals: Sequence[str],
    *,
    normalized_summary: str = "",
    section_type: str = "",
    title: str = "",
) -> str:
    """
    v4: adjunct procedure | post-proc recovery+formulation | multi | formulation | single | ambiguous.
    """
    _ = normalized_summary, section_type, title
    s = sentence or ""

    if _adjunct_procedure_hit(s):
        return "procedure_adjunct_combination"

    if _post_procedure_recovery_hit(s) and _formulation_hit(s):
        return "post_procedure_recovery_formulation"

    if _BROAD_PROCEDURE_RE.search(s) and not _formulation_hit(s):
        return "procedure_adjunct_combination"

    primary = _primary_clause(s, ingredient_name)
    clause = primary if primary else s
    tags_pc = _ingredient_tags_in_clause(clause, allowed_canonicals)

    if len(tags_pc) >= 2 and _clause_suggests_multi_actives(clause, tags_pc):
        return "multi_active_combination"

    ing_ok = _ingredient_in_sentence(ingredient_name, clause) or _ingredient_in_sentence(
        ingredient_name, s
    )

    if _formulation_hit(clause) or _formulation_hit(s):
        if len(tags_pc) <= 1 and ing_ok:
            return "single_formulation"

    if ing_ok:
        return "single_active"
    return "ambiguous"


def _significance_on_text(lower: str, claim_type: str, relation: str) -> str:
    if any(
        p in lower
        for p in (
            "no significant difference",
            "not statistically significant",
            "none reached statistical significance",
            "failed to reach significance",
            "not significant",
            "no significant ",
            "did not significantly",
            "no effect",
        )
    ):
        return "not_significant"
    if any(
        p in lower
        for p in (
            "statistically significant",
            "significant improvement",
            "significant reduction",
            "significantly reduced",
            "significantly improved",
            "p <",
            "p<",
            "p =",
            "p=",
        )
    ):
        return "significant"

    ct = (claim_type or "").strip().lower()
    rel = (relation or "").strip().lower()
    if ct == "safety" or rel in ("is_well_tolerated_for", "is_safe_for"):
        if any(x in lower for x in ("tolerat", "well-tolerated", "well tolerated", "safe")):
            return "not_applicable"
    if ct == "mechanism" and "efficacy" not in lower and "improv" not in lower:
        return "not_applicable"
    return "unclear"


def label_significance_v2(
    sentence: str,
    claim_type: str,
    relation: str,
    *,
    target: str = "",
) -> str:
    """v3 §5; prefer clause containing target tokens when possible."""
    s = sentence or ""
    lower_full = s.lower()
    target_words = [w for w in re.findall(r"[a-z]{4,}", (target or "").lower())]
    clauses = re.split(r"(?<=[.!?])\s+", s.strip()) if s.strip() else [s]
    scored = lower_full
    for cl in clauses:
        tl = cl.lower()
        if target_words and any(w in tl for w in target_words[:4]):
            scored = tl
            break
        if _efficacy_predicates_near(cl, 0, len(cl)):
            scored = tl
    return _significance_on_text(scored, claim_type, relation)


_BLOCKING_WEAK_SUBSTR = (
    "may ",
    "might ",
    "suggests",
    "promising",
    "could ",
    "potential",
    "possibly",
)

_STRONG_CUES = (
    "significantly improved",
    "significantly reduced",
    "statistically significant",
    "superior to placebo",
    "superior to control",
    "randomized trial showed",
    "double-blind study demonstrated",
    "clinically significant improvement",
    "rct ",
    "randomized controlled",
)

_WEAK_CUES = (
    "appears to",
    "appeared to",
    "trend toward",
    "proof-of-concept",
    "possibly",
)

_MODERATE_CUES = (
    "improved",
    "reduced",
    "enhanced",
    "promoted",
    "restored",
    "effective in",
    "demonstrated improvement",
    "showed efficacy",
    "was effective",
    "resulted in improvement",
    "effectively ",
    "decreased",
    "effective ",
    "well-tolerated",
    "well tolerated",
)


def label_strength_v2(sentence: str, hedging: bool, significance_label: str) -> str:
    """v3 §4: blocking weak cues forbid moderate even with CONCLUSION tone."""
    lower = (sentence or "").lower().strip()
    if significance_label == "not_significant":
        return "weak"

    has_strong = any(c in lower for c in _STRONG_CUES)
    has_blocking_weak = any(b in lower for b in _BLOCKING_WEAK_SUBSTR)
    has_weak_other = any(c in lower for c in _WEAK_CUES)
    has_weak = has_blocking_weak or has_weak_other
    has_moderate = any(c in lower for c in _MODERATE_CUES)

    conclusionish = (
        lower.startswith("conclusion")
        or lower.startswith("results:")
        or lower.startswith("result:")
        or "conclusion:" in lower[:50]
    )

    if has_strong:
        return "strong"
    if has_blocking_weak:
        return "weak"
    if has_weak and not has_strong:
        return "weak"
    if has_moderate:
        return "moderate"
    if conclusionish and has_moderate:
        return "moderate"
    if conclusionish and any(
        x in lower for x in ("effective", "decreased", "improvement", "repair", "barrier")
    ):
        return "moderate"
    if hedging:
        return "weak"
    return "weak"


def is_generalized_review_style(sentence: str, title: str, study_context: str) -> bool:
    blob = f"{sentence} {title}".lower()
    if (study_context or "").strip().lower() == "review":
        return True
    return any(
        x in blob
        for x in (
            "literature review",
            "systematic review",
            "narrative review",
            "scoping review",
            "review on efficacy",
        )
    )


def compute_eligibility_tier(
    strength_label: str,
    significance_label: str,
    attribution_label: str,
    claim_type: str,
    effect_ids: Sequence[int],
    concern_ids: Sequence[int],
    *,
    sentence: str = "",
    title: str = "",
    study_context: str = "",
    detected_labels: Sequence[str] = (),
) -> str:
    """
    v3 §6 + v4: strict needs single detected unit + no list-pattern / suspect;
    post_procedure_recovery_formulation → soft_graph like formulation.
    """
    _ = claim_type
    has_map = bool(effect_ids) or bool(concern_ids)
    is_review = is_generalized_review_style(sentence, title, study_context)
    list_block = has_strict_blocking_list_pattern(sentence)
    suspect = ingredient_detection_suspect(sentence, detected_labels)
    n_det = count_distinct_detection_units(detected_labels)
    strict_allowed = n_det == 1 and not list_block and not suspect

    if significance_label == "not_significant":
        return "evidence_only"
    if attribution_label == "ambiguous":
        return "evidence_only"
    if not has_map:
        return "evidence_only"

    if attribution_label in ("procedure_adjunct_combination", "procedure_combination"):
        return "recommendation_only"
    if attribution_label == "multi_active_combination":
        return "recommendation_only"
    if is_review:
        return "recommendation_only"

    if strength_label == "weak":
        return "recommendation_only"

    if (
        attribution_label == "single_active"
        and strength_label in ("strong", "moderate")
        and strict_allowed
    ):
        return "strict_graph"

    if (
        attribution_label == "single_active"
        and strength_label in ("strong", "moderate")
        and not strict_allowed
    ):
        return "recommendation_only"

    if (
        attribution_label == "single_formulation"
        and strength_label in ("strong", "moderate")
        and significance_label in ("significant", "unclear", "not_applicable")
    ):
        return "soft_graph"

    if (
        attribution_label == "post_procedure_recovery_formulation"
        and strength_label in ("strong", "moderate")
        and significance_label in ("significant", "unclear", "not_applicable")
    ):
        return "soft_graph"

    return "evidence_only"


def assert_tier_valid(tier: str) -> None:
    if tier not in _VALID_TIERS:
        raise ValueError(f"Invalid eligibility_tier: {tier!r}")


def is_graph_eligible_tier(eligibility_tier: str) -> bool:
    return eligibility_tier in _GRAPH_TIERS


def compute_row_weight(
    strength_label: str,
    significance_label: str,
    attribution_label: str,
    study_context: str | None,
) -> float:
    sw = _STRENGTH_WEIGHT.get(strength_label, 0.35)
    sig = _SIGNIFICANCE_WEIGHT.get(significance_label, 0.8)
    att = _ATTRIBUTION_WEIGHT.get(attribution_label, 0.2)
    sc = (study_context or "unknown").strip().lower()
    src = _STUDY_CONTEXT_WEIGHT.get(sc, 0.75)
    return round(sw * sig * att * src, 6)


def build_policy_reasons(
    eligibility_tier: str,
    attribution_label: str,
    strength_label: str,
    significance_label: str,
    has_mapping: bool,
    is_review: bool,
) -> Tuple[str, str]:
    """
    v3 §7: non-null policy strings for non graph-edge rows.
    Returns (exclusion_reason, recommendation_reason); use 'n/a' when not applicable.
    """
    if eligibility_tier in _GRAPH_TIERS:
        return "n/a", "n/a"
    if eligibility_tier == "evidence_only":
        parts: List[str] = []
        if not has_mapping:
            parts.append("no_taxonomy_mapping")
        if significance_label == "not_significant":
            parts.append("not_significant")
        if attribution_label == "ambiguous":
            parts.append("ambiguous_attribution")
        if strength_label == "weak":
            parts.append("weak_single_paper")
        if not parts:
            parts.append("evidence_only_policy")
        return ";".join(parts), "n/a"
    # recommendation_only
    reco: List[str] = []
    if attribution_label == "multi_active_combination":
        reco.append("multi_active_combination")
    if (
        attribution_label == "single_active"
        and strength_label in ("strong", "moderate")
    ):
        reco.append("strict_precision_guard")
    if attribution_label == "procedure_adjunct_combination":
        reco.append("procedure_combination")
    if attribution_label == "procedure_combination":
        reco.append("procedure_combination")
    if strength_label == "weak":
        reco.append("weak_single_paper")
    if is_review:
        reco.append("generalized_review_claim")
    if not reco:
        reco.append("below_graph_threshold")
    return "n/a", ";".join(reco)


def assert_canonical_score_order(canonical_rows: Sequence[Dict[str, Any]]) -> None:
    for row in canonical_rows:
        g = float(row.get("graph_score", 0.0))
        r = float(row.get("recommendation_score_base", 0.0))
        if r + 1e-9 < g:
            raise ValueError(
                f"recommendation_score_base < graph_score for {row.get('canonical_claim_key')}"
            )


def label_modality(claim_type: str, relation: str, sentence: str) -> str:
    ct = (claim_type or "").strip().lower()
    rel = (relation or "").strip().lower()
    lower = (sentence or "").lower()
    if ct == "mechanism":
        return "mechanism"
    if rel == "prevents" or lower.startswith("to prevent"):
        return "prevention"
    if ct == "safety" or rel in ("is_well_tolerated_for", "is_safe_for"):
        return "safety"
    if any(x in lower for x in ("formulation", "vehicle", "emulsion", "cream base")):
        return "formulation_support"
    return "efficacy"


def ids_to_pipe(ids: Iterable[int]) -> str:
    return "|".join(str(i) for i in sorted(set(ids)))


def aggregate_canonical_rows(
    batch_id: str,
    evidence_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    by_key: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in evidence_rows:
        by_key[row["canonical_claim_key"]].append(row)

    out: List[Dict[str, Any]] = []
    for ckey, rows in by_key.items():
        first = rows[0]
        study_vals = [str(r.get("study_context") or "unknown") for r in rows]
        study_distinct = len(set(study_vals))

        by_paper_all: Dict[str, float] = defaultdict(float)
        by_paper_graph: Dict[str, float] = defaultdict(float)
        by_paper_reco: Dict[str, float] = defaultdict(float)

        for r in rows:
            pmid = r["pmid"]
            w = float(r["row_weight"])
            tier = r.get("eligibility_tier") or "evidence_only"
            by_paper_all[pmid] = max(by_paper_all[pmid], w)
            if tier in _GRAPH_TIERS:
                by_paper_graph[pmid] = max(by_paper_graph[pmid], w)
            if tier in _RECO_TIERS:
                by_paper_reco[pmid] = max(by_paper_reco[pmid], w)

        paper_support_sum = sum(by_paper_all.values())
        paper_support_sum_graph = sum(by_paper_graph.values())
        paper_support_sum_reco = sum(by_paper_reco.values())

        pmids_graph = {r["pmid"] for r in rows if r.get("eligibility_tier") in _GRAPH_TIERS}
        pmids_reco = {r["pmid"] for r in rows if r.get("eligibility_tier") in _RECO_TIERS}

        graph_score = math.log(1.0 + paper_support_sum_graph)
        recommendation_score_base = math.log(1.0 + paper_support_sum_reco)
        evidence_score = math.log(1.0 + paper_support_sum)

        pe: set[int] = set()
        pc: set[int] = set()
        for r in rows:
            pe.update(r.get("effect_ids_list") or [])
            pc.update(r.get("concern_ids_list") or [])

        top_tier = "evidence_only"
        best_rank = -1
        for r in rows:
            t = r.get("eligibility_tier") or "evidence_only"
            rk = _TIER_RANK.get(t, 0)
            if rk > best_rank:
                best_rank = rk
                top_tier = t

        top_tier_rows = [r for r in rows if (r.get("eligibility_tier") or "") == top_tier]
        if top_tier_rows:
            cnt = Counter(str(r.get("attribution_label") or "") for r in top_tier_rows)
            top_attr = cnt.most_common(1)[0][0]
        else:
            top_attr = str(first.get("attribution_label") or "")

        any_graph = any(is_graph_eligible_tier(str(r.get("eligibility_tier") or "")) for r in rows)
        rel_norm = normalize_relation_for_canonical(str(first["relation"]))
        paper_distinct = len({r["pmid"] for r in rows})

        out.append(
            {
                "batch_id": batch_id,
                "canonical_claim_key": ckey,
                "ingredient_name": first["ingredient_name"],
                "relation": rel_norm,
                "target_normalized": first["target"],
                "target_category": first["target_category"],
                "primary_effect_ids": ids_to_pipe(pe),
                "primary_concern_ids": ids_to_pipe(pc),
                "evidence_count_raw": len(rows),
                "paper_count_distinct": paper_distinct,
                "study_count_distinct": study_distinct,
                "paper_support_sum": round(paper_support_sum, 6),
                "paper_support_sum_graph": round(paper_support_sum_graph, 6),
                "paper_support_sum_reco": round(paper_support_sum_reco, 6),
                "paper_count_graph": len(pmids_graph),
                "paper_count_reco": len(pmids_reco),
                "top_eligibility_tier": top_tier,
                "top_attribution_label": top_attr,
                "evidence_score": round(evidence_score, 6),
                "graph_score": round(graph_score, 6),
                "recommendation_score_base": round(recommendation_score_base, 6),
                "is_graph_eligible": any_graph,
            }
        )

    out.sort(key=lambda x: x["canonical_claim_key"])
    return out
