"""Offline regression suite: explicit source fixtures, not clinical approval.

Rebuild packets and legacy audits from original inputs; never trust a prior
report. Expected interpretations are separately supplied, hash-bound fixtures.
No inference, external calls, production export, or database connection.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from scripts.audit_evidence_gate import audit
from scripts.build_evidence_review_packets import build
from scripts.verify_urea_applicability import run as verify_urea

VERSION = "evidence-regression-suite-v1"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def lines(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def compare_packets(packets, expected):
    """Exact inventory plus independently specified meanings/blockers.

    Observation order is deliberately part of this frozen regression fixture.
    Source draft equality is checked separately by the caller.
    """
    if not expected or len(packets) != len(expected):
        raise ValueError("Incomplete packet expectation inventory")
    checks = []
    for index, (packet, want) in enumerate(zip(packets, expected)):
        if set(want) != {"pmid", "values", "effect", "required_blockers"} or not want["values"]:
            raise ValueError("Invalid packet expectation")
        fields = packet["draft"]["fields"]
        values = {key: fact["value"] if fact else None for key, fact in fields.items()}
        passed = (packet["source"]["pmid"] == want["pmid"]
                  and all(key in values and values[key] == value for key, value in want["values"].items())
                  and packet["proposed_effect_code"] == want["effect"]
                  and set(want["required_blockers"]).issubset(packet["pre_review_blockers"])
                  and packet["recommendation_eligible"] is False
                  and packet["status"] == "pending_independent_review")
        checks.append({"index": index, "observation_id": packet["observation_id"],
                       "expected": want, "actual_values": values,
                       "actual_effect": packet["proposed_effect_code"],
                       "actual_blockers": packet["pre_review_blockers"], "passed": passed})
    return checks


def run(*, claims, reviews, atomic_snapshot, atomic_annotations, snapshot,
        annotations, supplement, checklist, acceptance, expectations, output):
    if output.exists():
        raise FileExistsError("Use a new isolated suite output directory")
    inputs = dict(claims=claims, reviews=reviews, atomic_annotations=atomic_annotations,
                  annotations=annotations, supplement=supplement, checklist=checklist, acceptance=acceptance,
                  atomic_xml=atomic_snapshot / "pubmed.xml", control_xml=snapshot / "pubmed.xml",
                  atomic_manifest=atomic_snapshot / "manifest.json", control_manifest=snapshot / "manifest.json")
    hashes = {key: sha(path) for key, path in inputs.items()}
    spec = json.loads(expectations.read_text(encoding="utf-8"))
    expectations_hash = sha(expectations)
    if spec.get("version") != VERSION or spec.get("input_sha256") != hashes:
        raise ValueError("Expectations are not bound to these exact inputs")
    if set(spec.get("packets", {})) != {"atomic", "controls"} or not all(spec["packets"].values()):
        raise ValueError("Both outcome inventories are required")
    if type(spec.get("legacy_unique_records")) is not int or spec["legacy_unique_records"] < 1:
        raise ValueError("Positive legacy inventory size required")
    if not spec.get("rejected_record_ids") or len(set(spec["rejected_record_ids"])) != len(spec["rejected_record_ids"]):
        raise ValueError("Distinct rejected record identities required")
    review_rows = lines(reviews)
    if {r["record_id"] for r in review_rows} != set(spec["rejected_record_ids"]) or any(r.get("decision") != "reject" for r in review_rows):
        raise ValueError("This regression suite accepts explicit rejection fixtures only")
    # The constituent tools validate raw XML/spans/hashes again. Partial output
    # after an exception is not a successful suite: only final report establishes it.
    output.mkdir(parents=True)
    checks, all_packets = [], []
    for label, source, drafts in [("atomic", atomic_snapshot, atomic_annotations), ("controls", snapshot, annotations)]:
        build(source, drafts, output / label)
        packets = lines(output / label / "review_packets.jsonl")
        all_packets.extend(packets)
        checks.extend({"group": label, **c} for c in compare_packets(packets, spec["packets"][label]))
        checks.append({"group": label, "name": "original_drafts_preserved",
                       "passed": [p["draft"] for p in packets] == lines(drafts)})
    unreviewed = audit(claims, output / "unreviewed")
    reviewed = audit(claims, output / "reviewed", reviews)
    raw_decisions, decisions = lines(output / "unreviewed" / "decisions.jsonl"), lines(output / "reviewed" / "decisions.jsonl")
    expected_count = spec["legacy_unique_records"]
    checks.append({"group": "legacy", "name": "complete_unreviewed_inventory_no_candidates",
                   "passed": unreviewed["unique_records"] == expected_count
                   and unreviewed["status_counts"] == {"review_required": expected_count}})
    expected_ids = set(spec["rejected_record_ids"])
    checks.append({"group": "legacy", "name": "only_explicit_errors_rejected_other_rows_preserved",
                   "passed": reviewed["unique_records"] == expected_count
                   and len(decisions) == len(raw_decisions)
                   and [d["source"] for d in decisions] == [d["source"] for d in raw_decisions]
                   and {d["record_id"] for d in decisions if d["status"] == "reject"} == expected_ids
                   and all(d["status"] == ("reject" if d["record_id"] in expected_ids else "review_required") for d in decisions)})
    checks.append({"group": "legacy", "name": "no_candidate_edge_export",
                   "passed": not lines(output / "unreviewed" / "candidate_edges.jsonl")
                   and not lines(output / "reviewed" / "candidate_edges.jsonl")})
    urea = verify_urea(snapshot=snapshot, annotations=annotations, supplement=supplement,
                       checklist=checklist, acceptance=acceptance, output=output / "urea")
    checks.extend({"group": "urea", **c} for c in json.loads((output / "urea" / "applicability_report.json").read_text())["checks"])
    checks.append({"group": "inputs", "name": "input_files_unchanged",
                   "passed": hashes == {k: sha(p) for k, p in inputs.items()} and expectations_hash == sha(expectations)})
    report = {"version": VERSION, "code_sha": urea["code_sha"], "code_dirty": urea["code_dirty"],
              "input_sha256": hashes, "expectations_sha256": expectations_hash,
              "passed": sum(c["passed"] for c in checks), "failed": sum(not c["passed"] for c in checks),
              "packet_count": len(all_packets), "observed_pmid_count": len({p["source"]["pmid"] for p in all_packets}),
              "legacy_unique_records": reviewed["unique_records"],
              "rejected_records": sum(d["status"] == "reject" for d in decisions),
              "recommendation_eligible_count": sum(p["recommendation_eligible"] is True for p in all_packets),
              "candidate_edge_count": len(lines(output / "reviewed" / "candidate_edges.jsonl")),
              "production_ready": False,
              "warning": "Regression against provisional interpretations, not independent clinical validation, recall or response quality. No graph export.",
              "checks": checks}
    (output / "suite_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {k: v for k, v in report.items() if k != "checks"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("claims", "reviews", "atomic_snapshot", "atomic_annotations", "snapshot", "annotations",
                "supplement", "checklist", "acceptance", "expectations", "output"):
        parser.add_argument("--" + key.replace("_", "-"), type=Path, required=True)
    result = run(**vars(parser.parse_args()))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return int(result["failed"] > 0)


if __name__ == "__main__":
    raise SystemExit(main())
