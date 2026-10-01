"""Offline integration: real source transformation x synthetic context matrix.

No actual users, inferred diagnoses, recommendations, LLM calls or database writes.
"""
import argparse
import copy
import json
from pathlib import Path

from pipeline.gold.claim.shadow_applicability import VERSION, assess
from scripts.transform_reviewed_urea_pilot import run as transform_run

BASE_CONTEXT = {"population": "ichthyosis_vulgaris", "body_site": "legs", "ingredient_name": "Urea",
                "route": "topical", "concentration_percent": 7.5, "duration_days": 28,
                "formulation": "study_matched_base_cream", "effect_code": "HYDRATING"}


def scenarios():
    changes = [
        ("exact_study_context", {}),
        ("healthy_facial_skin", {"population": "healthy_skin", "body_site": "face"}),
        ("arms_not_legs", {"body_site": "arms"}),
        ("unknown_population", {"population": None}),
        ("unknown_site", {"body_site": None}),
        ("unknown_concentration", {"concentration_percent": None}),
        ("different_concentration", {"concentration_percent": 10}),
        ("different_formulation", {"formulation": "unspecified_marketed_product"}),
        ("unknown_duration", {"duration_days": None}),
        ("water_loss_not_hydration", {"effect_code": "MOISTURE_RETENTION"}),
        ("barrier_repair_not_hydration", {"effect_code": "BARRIER_REPAIR"}),
        ("oral_not_topical", {"route": "oral"}),
        ("different_ingredient", {"ingredient_name": "Panthenol"}),
        ("different_duration", {"duration_days": 7}),
    ]
    return [{"id": name, "query": {**BASE_CONTEXT, **patch}} for name, patch in changes]


def verify(outcomes):
    if len(outcomes) != 3 or {r["case_id"] for r in outcomes} != {
        "urea-legs-hydration", "urea-arms-hydration", "urea-tewl-excluded"}:
        raise ValueError("Expected the complete three-outcome pilot")
    before = copy.deepcopy(outcomes)
    checks = []
    for scenario in scenarios():
        for outcome in outcomes:
            expected_match = scenario["id"] == "exact_study_context" and outcome["case_id"] == "urea-legs-hydration"
            decision = assess(outcome, scenario["query"])
            passed = (decision["context_match"] == expected_match and decision["evidence_use_allowed"] is False
                      and decision["status"] == ("context_matched_review_pending" if expected_match else "blocked"))
            checks.append({"scenario_id": scenario["id"], "case_id": outcome["case_id"],
                           "query": scenario["query"], "expected_context_match": expected_match,
                           "decision": decision, "passed": passed})
    if outcomes != before:
        raise AssertionError("Applicability check mutated source outcomes")
    return checks


def run(**args):
    output = args["output"]
    manifest = transform_run(**args)
    outcomes = [json.loads(line) for line in (output / "outcomes.jsonl").read_text(encoding="utf-8").splitlines()]
    checks = verify(outcomes)
    report = {"policy_version": VERSION, "code_sha": manifest["code_sha"], "code_dirty": manifest["code_dirty"],
              "input_sha256": manifest["input_sha256"], "scenario_count": len(scenarios()),
              "check_count": len(checks), "passed": sum(c["passed"] for c in checks),
              "failed": sum(not c["passed"] for c in checks),
              "context_matches": sum(c["decision"]["context_match"] for c in checks),
              "evidence_use_allowed": sum(c["decision"]["evidence_use_allowed"] for c in checks),
              "scope": "actual_source_records_x_synthetic_normalized_contexts_not_user_response_eval",
              "checks": checks}
    with (output / "applicability_report.json").open("x", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return {k: v for k, v in report.items() if k != "checks"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("snapshot", "annotations", "supplement", "checklist", "acceptance", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    result = run(**vars(parser.parse_args()))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return int(result["failed"] > 0)


if __name__ == "__main__":
    raise SystemExit(main())
