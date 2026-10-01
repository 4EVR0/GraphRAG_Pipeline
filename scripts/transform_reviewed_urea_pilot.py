"""Transform the three agreed urea classifications into isolated test records.

Deliberately a ONE-PAPER pilot, not a generic clinical approval engine. No network,
LLM, scoring, or database access. Classification agreement never authorizes use
in recommendations. Full-text provenance stays separate from abstract provenance.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

from pipeline.gold.claim.evidence_gate import record_id
from scripts.build_evidence_review_packets import load_snapshot, make_packet

VERSION = "reviewed-urea-shadow-v1"
CASES = {
    "urea-legs-hydration": ("conditional_positive_hydration_evidence", "legs", "supported", "increase", "significant"),
    "urea-arms-hydration": ("no_detected_between_treatment_difference", "arms", "no_detected_effect", "no_detected_difference", "not_significant"),
    "urea-tewl-excluded": ("outcome_excluded_not_reported", None, "excluded_not_reported", None, None),
}
PASSAGES = {"same_base", "application_conditions", "legs_hydration_difference", "arms_no_detected_difference", "tewl_excluded"}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def indexed(items: list, key: str) -> dict:
    require(isinstance(items, list), "Expected a list")
    result = {}
    for item in items:
        require(isinstance(item, dict) and isinstance(item.get(key), str) and item[key] not in result,
                "Missing or duplicate identity")
        result[item[key]] = item
    return result


def transform(snapshot: Path, annotations: Path, supplement: Path, checklist: Path, acceptance: Path) -> tuple[list, dict]:
    paths = {"annotations": annotations, "supplement": supplement, "checklist": checklist, "acceptance": acceptance}
    raw = {key: path.read_bytes() for key, path in paths.items()}
    hashes = {key: digest(value) for key, value in raw.items()}
    sup, check, accept = (json.loads(raw[key]) for key in ("supplement", "checklist", "acceptance"))
    require(sup.get("schema_version") == "local-selected-passage-supplement-v1", "Unknown supplement version")
    require(check.get("version") == "urea-human-review-checklist-v1", "Unknown checklist version")
    require(accept.get("schema_version") == "user-classification-acceptance-v1", "Unknown acceptance version")
    require(accept.get("review_checklist_sha256") == hashes["checklist"], "Checklist changed after agreement")
    require(accept.get("source_supplement_sha256") == check.get("source_supplement_sha256") == hashes["supplement"],
            "Supplement hash mismatch")
    require(sup.get("parent_annotations_sha256") == hashes["annotations"], "Parent annotations changed")
    for data in (sup, check):
        require(data.get("pmid") == "35663767" and data.get("pmcid_version") == "PMC9060062.1", "Outside approved one-paper pilot")
    require(sup.get("doi") == "10.1002/ski2.65", "DOI mismatch")
    require(sup.get("source_url") == "https://pmc.ncbi.nlm.nih.gov/articles/PMC9060062/", "Source URL mismatch")
    require(sup.get("license") == "CC BY 4.0" and sup.get("license_url") == "https://creativecommons.org/licenses/by/4.0/",
            "Unexpected license metadata")
    require(isinstance(accept.get("reviewer"), str) and bool(accept["reviewer"].strip()), "Missing classification reviewer")
    require(all(accept.get(key) is False for key in ("production_recommendation_approved", "clinical_validity_review_completed", "is_gate_approval_file")),
            "This transform accepts classification agreement only")
    require(sup.get("recommendation_eligible") is False, "Supplement must not assert recommendation eligibility")
    decisions, checks = indexed(accept.get("cases"), "id"), indexed(check.get("cases"), "id")
    require(set(decisions) == set(checks) == set(CASES), "Incomplete or unexpected classification set")

    manifest, papers = load_snapshot(snapshot)
    require(manifest["response_sha256"] == sup.get("parent_abstract_response_sha256"), "Abstract source changed")
    parents = {}
    for line in raw["annotations"].decode("utf-8").splitlines():
        if not line.strip():
            continue
        draft = json.loads(line)
        require(draft.get("pmid") in papers, "Unknown parent PMID")
        packet = make_packet(draft, papers[draft["pmid"]], manifest["response_sha256"])
        require(packet["observation_id"] not in parents, "Duplicate parent observation")
        parents[packet["observation_id"]] = packet
    parent_refs = indexed(sup.get("parent_observations"), "body_site")
    require(set(parent_refs) == {"legs", "arms"}, "Expected two body-site observations")
    for site, ref in parent_refs.items():
        packet = parents.get(ref.get("observation_id"))
        require(packet is not None and packet["draft_sha256"] == ref.get("draft_sha256"), "Parent identity mismatch")
        fields = packet["draft"]["fields"]
        require(packet["source"]["pmid"] == "35663767" and fields["body_site"]["value"] == site,
                "Cross-paper or cross-site parent")
        require(fields["ingredient_name"]["value"] == "Urea" and fields["measured_endpoint"]["value"] == "skin_water_content",
                "Unexpected parent ingredient/endpoint")

    passages = indexed(sup.get("selected_passages"), "id")
    require(set(passages) == PASSAGES, "Missing or unexpected selected passage")
    for p in passages.values():
        require(digest(p["text"].encode()) == p["text_sha256"] and digest(p["parsed_fragment_xml"].encode()) == p["fragment_sha256"],
                "Passage hash mismatch")
        projected = " ".join(" ".join(ET.fromstring(p["parsed_fragment_xml"]).itertext()).split())
        require(projected == p["text"], "Text/XML disagreement")
    expected_updates = {"study_control": "matched_vehicle", "attribution": "isolated_ingredient", "result_comparator": "matched_vehicle"}
    updates = sup.get("proposed_context_updates", {})
    require(set(updates) == set(expected_updates), "Unexpected context update")
    for field, value in expected_updates.items():
        require(updates[field].get("value") == value and "same_base" in updates[field].get("passage_ids", [])
                and set(updates[field]["passage_ids"]).issubset(passages), "Context update lacks source binding")

    outcomes = []
    for case_id, (classification, site, support, direction, significance) in CASES.items():
        decision, item = decisions[case_id], checks[case_id]
        require(decision.get("decision") == "accept_presented_classification" and decision.get("classification") == classification
                and item.get("proposed_classification") == classification, "Classification not agreed")
        needed = {"tewl_excluded"} if site is None else {"same_base", "legs_hydration_difference" if site == "legs" else "arms_no_detected_difference"}
        refs = item.get("source_passages", [])
        require(needed.issubset(refs) and set(refs).issubset(passages), "Case lacks relevant passages")
        require(set(indexed(item.get("source_locations"), "id")) == set(refs), "Passage location set mismatch")
        for loc in item["source_locations"]:
            require(all(passages[loc["id"]].get(k) == v for k, v in loc.items()), "Source locator mismatch")
        require(isinstance(item.get("forbidden_generalizations"), list) and item["forbidden_generalizations"], "Restrictions missing")
        parent = parents[parent_refs[site]["observation_id"]] if site else None
        if parent:
            fields = parent["draft"]["fields"]
            for field, expected in (("result_support", support), ("change_direction", direction), ("significance", significance)):
                require(fields[field]["value"] == expected, "Parent result conflicts with agreed classification")
            constraints = {key: fields[key] for key in ("population", "body_site", "concentration", "formulation", "duration")}
        else:
            # Excluded measurement has no positive direction, p-value or invented
            # body-specific result. Do not copy hydration's measurements here.
            constraints = None
        record = {
            "transform_version": VERSION, "case_id": case_id, "classification": classification,
            "pmid": "35663767", "pmcid_version": sup["pmcid_version"], "doi": sup["doi"],
            "ingredient_name": "Urea", "measured_endpoint": "skin_water_content" if site else "transepidermal_water_loss",
            "result_support": support, "change_direction": direction, "significance": significance,
            "effect_code": "HYDRATING" if site == "legs" else None,
            "constraints": constraints, "parent_observation": parent,
            "context_supplement": {
                "updates": {field: {"value": value["value"],
                    "passage_ids": [ref for ref in value["passage_ids"] if ref in refs]}
                    for field, value in updates.items()},
                "pmcid_version": sup["pmcid_version"],
            } if site else None,
            "selected_passages": [passages[ref] for ref in refs],
            "source_url": sup["source_url"], "license": sup["license"], "license_url": sup["license_url"],
            "input_sha256": hashes, "abstract_response_sha256": manifest["response_sha256"],
            "fulltext_response_sha256": sup["response_sha256"],
            "review_scope": "user_agreed_presented_classification_only",
            "independent_full_source_read_confirmed": accept.get("independent_full_source_read_confirmed") is True,
            "clinical_validity_review_completed": False, "recommendation_eligible": False,
            "use": "isolated_regression_fixture_not_production_evidence",
            "restrictions": item["forbidden_generalizations"],
            "statistical_reporting_note": sup["interpretation"]["statistical_reporting_note"],
        }
        record["outcome_id"] = record_id(record)
        outcomes.append(record)
    return outcomes, hashes


def run(snapshot: Path, annotations: Path, supplement: Path, checklist: Path, acceptance: Path, output: Path) -> dict:
    require(not output.exists(), "Use a new isolated output directory")
    outcomes, hashes = transform(snapshot, annotations, supplement, checklist, acceptance)
    repo = Path(__file__).resolve().parents[1]
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True)
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=repo, capture_output=True, text=True, check=True)
    manifest = {"version": VERSION, "created_at": datetime.now(timezone.utc).isoformat(),
                "code_sha": sha.stdout.strip(), "code_dirty": bool(dirty.stdout.strip()), "input_sha256": hashes,
                "outcome_count": len(outcomes), "classification_counts": {r["classification"]: 1 for r in outcomes},
                "recommendation_eligible_count": 0, "mode": "isolated_regression_only_no_gate_approval"}
    output.mkdir(parents=True)
    with (output / "outcomes.jsonl").open("x", encoding="utf-8") as handle:
        for record in outcomes:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    with (output / "manifest.json").open("x", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("snapshot", "annotations", "supplement", "checklist", "acceptance", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(**vars(args)), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
