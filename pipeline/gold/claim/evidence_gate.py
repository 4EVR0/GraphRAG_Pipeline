"""Fail-closed, offline evidence review gate (not wired to production exports).

Legacy extraction labels are hints, not proof of study scope or causality.
Only source-bound, explicitly approved reviews can produce candidate edges.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import date
from typing import Mapping

GATE_VERSION = "skin-evidence-gate-v1"
SCHEMA_VERSION = "reviewed-claim-v1"
PILOT_EFFECTS = frozenset({"HYDRATING", "MOISTURE_RETENTION", "BARRIER_REPAIR"})
MODEL_SUBJECTS = frozenset({"skin_cells", "reconstructed_skin", "animal_skin", "ex_vivo_skin"})
NON_SKIN = re.compile(
    r"bread packaging|food packaging|\bRTgutGC\b|fish gut|intestinal mucosa|"
    r"esophag\w*|oesophag\w*|gastroesophageal|poly\s*\(vinyl alcohol\)", re.I
)
OUTCOMES = {
    "skin_water_content": ("HYDRATING", "increase"),
    "transepidermal_water_loss": ("MOISTURE_RETENTION", "decrease"),
    "skin_barrier_recovery": ("BARRIER_REPAIR", "improve"),
}


def record_id(row: Mapping) -> str:
    """Bind a review to the entire source CSV row, including extraction versions.

    Any correction or batch change invalidates approval; do not guess that two
    similar claims have equivalent provenance. File hash/row number live in audit.
    """
    payload = json.dumps(dict(row), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def text(row: Mapping, key: str) -> str:
    return str(row.get(key) or "").strip()


@dataclass(frozen=True)
class Decision:
    record_id: str
    status: str
    reasons: tuple[str, ...]
    effect_code: str | None = None
    gate_version: str = GATE_VERSION

    def to_dict(self) -> dict:
        return asdict(self)


def evaluate(row: Mapping, review: Mapping | None = None) -> Decision:
    """Return reject / review_required / mechanism_only / candidate.

    Text patterns only flag legacy rows for review. They never establish clinical
    validity or silently repair a direction error. A reviewer must attest the
    evidence and check subject, endpoint, attribution, comparator and limitations.
    """
    rid = record_id(row)

    def result(status: str, *reasons: str, effect: str | None = None) -> Decision:
        return Decision(rid, status, tuple(reasons), effect)

    if not re.fullmatch(r"[1-9][0-9]*", text(row, "pmid")) or not all(
        text(row, key) for key in ("ingredient_name", "source_sentence", "title")
    ):
        return result("review_required", "missing_source_identity")

    if review is None:
        flags = ["source_bound_review_required"]
        context = text(row, "title") + " " + text(row, "source_sentence")
        if NON_SKIN.search(context):
            flags.append("suspected_non_skin_context")
        if text(row, "study_context") in {"unknown", ""}:
            flags.append("unknown_study_context")
        if text(row, "claim_type") == "safety":
            flags.append("safety_is_not_efficacy")
        if text(row, "relation") in {"reduces", "inhibits", "decreases"} and text(
            row, "target"
        ).lower() in {"skin barrier function", "hydration", "skin hydration"}:
            flags.append("endpoint_direction_requires_review")
        return result("review_required", *flags)

    if review.get("schema_version") != SCHEMA_VERSION or review.get("record_id") != rid:
        return result("review_required", "review_version_or_source_mismatch")
    try:
        reviewed_on = date.fromisoformat(text(review, "reviewed_on"))
        valid_date = reviewed_on <= date.today()
    except ValueError:
        valid_date = False
    if not text(review, "reviewer") or not valid_date:
        return result("review_required", "reviewer_and_date_required")
    if review.get("decision") == "reject":
        if not text(review, "review_note"):
            return result("review_required", "rejection_note_required")
        return result("reject", "reviewer_rejected")
    if review.get("decision") != "approve" or review.get("source_verified") is not True:
        return result("review_required", "explicit_source_verification_required")

    # Reviews may correct legacy extractions, but must retain a literal source span
    # and the review rationale. A free-form LLM verdict is not an approval.
    quote = text(review, "supporting_quote")
    if not quote or quote not in text(row, "source_sentence") or not text(review, "source_locator"):
        return result("review_required", "source_span_required")
    if not text(review, "review_note"):
        return result("review_required", "review_rationale_required")
    subject = text(review, "subject")
    if subject == "non_skin":
        return result("reject", "non_skin_subject")
    if subject not in MODEL_SUBJECTS | {"human_skin"}:
        return result("review_required", "unknown_subject")
    if NON_SKIN.search(text(row, "title") + " " + text(row, "source_sentence")):
        # Mixed tissue studies are not automatically rejected, but require an
        # explicit reviewer explanation rather than accepting a skin keyword.
        if not text(review, "scope_resolution"):
            return result("review_required", "conflicting_scope_requires_resolution")

    if subject in MODEL_SUBJECTS:
        if review.get("claim_kind") not in {"efficacy", "mechanism"} or review.get("claim_entailment_verified") is not True:
            return result("review_required", "model_claim_review_required")
        return result("mechanism_only", "skin_model_excluded_from_ranking")
    if review.get("route") != "topical":
        return result("review_required", "non_topical_or_unknown_route")
    if review.get("claim_kind") != "efficacy":
        return result("review_required", "not_an_efficacy_claim")
    if review.get("attribution") != "isolated_ingredient":
        return result("review_required", "ingredient_contribution_unresolved")
    if review.get("comparator") not in {"matched_vehicle", "ingredient_add_on_control"}:
        return result("review_required", "ingredient_isolating_comparator_required")
    if review.get("effect_code") not in PILOT_EFFECTS:
        return result("review_required", "outside_hydration_pilot")
    expected = OUTCOMES.get(text(review, "measured_endpoint"))
    if expected != (review.get("effect_code"), review.get("change_direction")):
        return result("review_required", "endpoint_direction_effect_mismatch")
    if review.get("result_support") != "supported" or review.get("significance") != "significant":
        return result("review_required", "no_verified_positive_result")
    if review.get("claim_entailment_verified") is not True:
        return result("review_required", "claim_entailment_review_required")
    if not all(text(review, key) for key in (
        "population", "body_site", "concentration", "formulation", "duration",
        "context_excerpt", "context_source_url",
    )):
        return result("review_required", "applicability_context_required")
    if text(review, "context_source_url") != f"https://pubmed.ncbi.nlm.nih.gov/{text(row, 'pmid')}/":
        return result("review_required", "context_pmid_mismatch")
    if any(text(review, key).lower() in {"unknown", "n/a", "not reported"} for key in (
        "population", "body_site", "concentration", "formulation", "duration"
    )):
        return result("review_required", "applicability_context_unknown")
    limitations = review.get("limitations")
    if not isinstance(limitations, list) or not limitations or not all(
        isinstance(item, str) and item.strip() for item in limitations
    ):
        return result("review_required", "limitations_required")
    return result("candidate", "reviewed_human_topical_evidence", effect=review["effect_code"])


def candidate_edge(row: Mapping, review: Mapping, decision: Decision) -> dict:
    """Provenance-rich claim edge, deliberately NOT a bulk Neo4j import row.

    No graph_score: clinical effect size and search priority are separate policies.
    No INCI guess: ingredient identity must be resolved before any graph import.
    """
    if decision.status != "candidate" or evaluate(row, review) != decision:
        raise ValueError("Only current source-bound candidates can become shadow edges")
    return {
        "ingredient_name": text(row, "ingredient_name"),
        "effect_code": decision.effect_code,
        "normalized_direction": "beneficial",
        "measured_endpoint": review["measured_endpoint"],
        "observed_change": review["change_direction"],
        "pmid": text(row, "pmid"),
        "record_id": decision.record_id,
        "source_evidence_id": text(row, "evidence_id"),
        "source_batch": text(row, "gold_batch_id") or text(row, "batch_id"),
        "gate_version": GATE_VERSION,
        "review_schema_version": SCHEMA_VERSION,
        "review_sha256": record_id(review),
        "source_url": f"https://pubmed.ncbi.nlm.nih.gov/{text(row, 'pmid')}/",
        "use": "candidate_only_requires_identity_and_query_applicability_check",
        "constraints": {key: review[key] for key in (
            "population", "body_site", "concentration", "formulation", "duration", "limitations"
        )},
    }
