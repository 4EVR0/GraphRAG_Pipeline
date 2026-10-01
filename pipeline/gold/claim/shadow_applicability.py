"""Fail-closed context matching for isolated reviewed-urea-shadow-v2 records.

Not wired into retrieval. Inputs are explicitly normalized test contexts, not
diagnoses or classifications inferred from a user's natural-language question.
Even matching context cannot promote these fixtures to production evidence.
"""
from __future__ import annotations

import math
from collections.abc import Mapping

from pipeline.gold.claim.evidence_gate import record_id

VERSION = "urea-shadow-applicability-v1"
TEXT_KEYS = ("population", "body_site", "ingredient_name", "route", "formulation")
NUMBER_KEYS = ("concentration_percent", "duration_days")
QUERY_KEYS = frozenset((*TEXT_KEYS, *NUMBER_KEYS, "effect_code"))


def assess(outcome: Mapping, query: Mapping) -> dict:
    result = {"policy_version": VERSION, "outcome_id": outcome.get("outcome_id"),
              "status": "blocked", "context_match": False, "evidence_use_allowed": False, "reasons": []}

    def blocked(*reasons):
        return {**result, "reasons": list(reasons)}

    payload = {key: value for key, value in outcome.items() if key != "outcome_id"}
    if outcome.get("outcome_id") != record_id(payload):
        return blocked("outcome_integrity_mismatch")
    if outcome.get("transform_version") != "reviewed-urea-shadow-v2" or outcome.get("use") != "isolated_regression_fixture_not_production_evidence":
        return blocked("unsupported_record_contract")
    if outcome.get("recommendation_eligible") is not False or outcome.get("clinical_validity_review_completed") is not False:
        return blocked("unexpected_approval_state")
    support = outcome.get("result_support")
    if support == "excluded_not_reported":
        return blocked("outcome_excluded_not_reported")
    if support != "supported":
        return blocked("no_positive_between_treatment_result")
    if (outcome.get("classification"), outcome.get("measured_endpoint"), outcome.get("change_direction"),
        outcome.get("significance"), outcome.get("effect_code")) != (
            "conditional_positive_hydration_evidence", "skin_water_content", "increase", "significant", "HYDRATING"):
        return blocked("inconsistent_positive_outcome")
    scope = outcome.get("applicability_scope")
    # Scope profile is deliberately bounded to this one-paper test. A changed
    # population/site cannot silently broaden it even if a hash is recomputed.
    expected_scope = {"population": "ichthyosis_vulgaris", "body_site": "legs", "ingredient_name": "Urea",
                      "route": "topical", "concentration_percent": 7.5, "duration_days": 28,
                      "formulation": "study_matched_base_cream"}
    if scope != expected_scope or outcome.get("pmid") != "35663767":
        return blocked("unsupported_study_scope")
    if not isinstance(query, Mapping):
        return blocked("invalid_query_context")
    missing, unexpected = QUERY_KEYS - set(query), set(query) - QUERY_KEYS
    if missing or unexpected:
        return blocked(*(["missing_query_fields:" + ",".join(sorted(missing))] if missing else []),
                       *(["unexpected_query_fields"] if unexpected else []))
    for key in (*TEXT_KEYS, "effect_code"):
        if not isinstance(query[key], str) or not query[key].strip():
            return blocked("unknown_or_invalid:" + key)
    for key in NUMBER_KEYS:
        value = query[key]
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            return blocked("unknown_or_invalid:" + key)
    mismatches = ["mismatch:" + key for key, value in scope.items() if query[key] != value]
    if query["effect_code"] != outcome["effect_code"]:
        mismatches.append("mismatch:effect_code")
    if mismatches:
        return blocked(*mismatches)
    return {**result, "status": "context_matched_review_pending", "context_match": True,
            "reasons": ["classification_agreement_is_not_clinical_or_production_approval"]}
