from typing import Any, Dict, Optional, Set

from pipeline.claim.services.claim_extractor import extractor


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

WEAK_TARGETS = {
    "benefit",
    "benefits",
    "outcome",
    "outcomes",
    "condition",
    "conditions",
    "efficacy",
    "safety",
    "improvement",
    "result",
    "results",
    "therapy",
    "treatment",
}


def normalize_and_validate_claim(
    claim: Dict[str, Any],
    sentence: str,
    allowed_ingredients: Set[str],
) -> Optional[Dict[str, Any]]:
    required_fields = {
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
    }

    if not required_fields.issubset(claim.keys()):
        return None

    raw_ingredient = str(claim["ingredient"]).strip()
    claim_type = str(claim["claim_type"]).strip()
    relation = str(claim["relation"]).strip()
    target = str(claim["target"]).strip()
    target_category = str(claim["target_category"]).strip()
    evidence_direction = str(claim["evidence_direction"]).strip()
    study_context = str(claim["study_context"]).strip()

    if not raw_ingredient or not target:
        return None

    normalized_ingredient = extractor.normalize_ingredient_name(raw_ingredient)
    if normalized_ingredient is None:
        return None

    if normalized_ingredient not in allowed_ingredients:
        return None

    if claim_type not in ALLOWED_CLAIM_TYPES:
        return None
    if relation not in ALLOWED_RELATIONS:
        return None
    if target_category not in ALLOWED_TARGET_CATEGORIES:
        return None
    if evidence_direction not in ALLOWED_EVIDENCE_DIRECTIONS:
        return None
    if study_context not in ALLOWED_STUDY_CONTEXTS:
        return None

    try:
        confidence = float(claim["confidence"])
    except (TypeError, ValueError):
        return None

    confidence = max(0.0, min(1.0, confidence))
    hedging = bool(claim["hedging"])
    negation = bool(claim["negation"])

    if target.lower() in WEAK_TARGETS:
        return None

    return {
        "ingredient": normalized_ingredient,
        "claim_type": claim_type,
        "relation": relation,
        "target": target,
        "target_category": target_category,
        "evidence_direction": evidence_direction,
        "evidence_text": sentence.strip(),
        "study_context": study_context,
        "hedging": hedging,
        "negation": negation,
        "confidence": confidence,
    }