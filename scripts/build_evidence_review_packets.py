"""Bind manually drafted atomic outcomes to a verified local PubMed snapshot.

Offline only. No NLP truth verification, approval, legacy-claim rewrite or graph
export. Every populated finding has a literal abstract span; unknowns stay null.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from pipeline.gold.claim.evidence_gate import MODEL_SUBJECTS, OUTCOMES, record_id
from scripts.fetch_pubmed_review_sources import VERSION as SNAPSHOT_VERSION, parse_articles

VERSION = "atomic-outcome-review-v1"
CONTEXT = ("population", "body_site", "concentration", "formulation", "duration")
ENUMS = {
    "subject": {"human_skin", "non_skin", *MODEL_SUBJECTS},
    "route": {"topical", "oral", "other"},
    "claim_kind": {"efficacy", "safety", "mechanism"},
    "attribution": {"isolated_ingredient", "combination", "unresolved"},
    "study_control": {"matched_vehicle", "ingredient_add_on_control", "active_control", "baseline", "other"},
    # The reported outcome contrast is NOT inferred from study_control.
    "result_comparator": {"matched_vehicle", "ingredient_add_on_control", "active_control", "baseline", "between_populations", "other"},
    "measured_endpoint": {*OUTCOMES, "irritation", "other"},
    "change_direction": {"increase", "decrease", "improve", "no_detected_difference", "other"},
    "result_support": {"supported", "no_detected_effect", "unclear"},
    "significance": {"significant", "not_significant", "not_reported"},
}
FIELDS = ("ingredient_name", *ENUMS, *CONTEXT)


def load_snapshot(directory: Path) -> tuple[dict, dict]:
    """Reparse raw XML, not mutable derived papers.jsonl. Check capture manifest."""
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    raw = (directory / "pubmed.xml").read_bytes()
    if manifest.get("snapshot_version") != SNAPSHOT_VERSION or hashlib.sha256(raw).hexdigest() != manifest.get("response_sha256"):
        raise ValueError("Snapshot version or raw response hash mismatch")
    papers = parse_articles(raw, manifest["requested_pmids"])
    if manifest.get("article_count") != len(papers):
        raise ValueError("Snapshot article count mismatch")
    return manifest, {p["pmid"]: p for p in papers}


def validate_span(span: dict, paper: dict) -> None:
    if not isinstance(span, dict) or set(span) != {"section", "quote"}:
        raise ValueError("Span requires section index and literal quote")
    index, quote = span["section"], span["quote"]
    if type(index) is not int or not 0 <= index < len(paper["abstract_sections"]):
        raise ValueError("Invalid abstract section index")
    if not isinstance(quote, str) or not quote.strip() or quote not in paper["abstract_sections"][index]["text"]:
        raise ValueError("Quote is not present in this exact abstract section")


def make_packet(draft: dict, paper: dict, source_hash: str) -> dict:
    required = {"schema_version", "source_response_sha256", "pmid", "prepared_by", "prepared_on", "fields", "result_span", "limitations", "note"}
    if not isinstance(draft, dict) or set(draft) != required:
        raise ValueError("Unexpected or missing draft keys; approvals are not accepted")
    if draft["schema_version"] != VERSION or draft["source_response_sha256"] != source_hash or draft["pmid"] != paper["pmid"]:
        raise ValueError("Draft version or source mismatch")
    for key in ("prepared_by", "prepared_on", "note"):
        if not isinstance(draft[key], str) or not draft[key].strip():
            raise ValueError(f"Missing {key}")
    if datetime.fromisoformat(draft["prepared_on"]).date() > datetime.now(timezone.utc).date():
        raise ValueError("Future preparation date")
    if not isinstance(draft["limitations"], list) or not draft["limitations"] or any(not isinstance(x, str) or not x.strip() for x in draft["limitations"]):
        raise ValueError("Explicit limitations required")
    fields = draft["fields"]
    if not isinstance(fields, dict) or set(fields) != set(FIELDS):
        raise ValueError("Every field must be present; use null for missing information")
    values = {}
    for key, fact in fields.items():
        if fact is None:
            values[key] = None
            continue
        if not isinstance(fact, dict) or set(fact) != {"value", "spans"}:
            raise ValueError(f"Invalid fact: {key}")
        value = fact["value"]
        if not isinstance(value, str) or not value.strip() or value.lower() in {"unknown", "n/a", "not reported"}:
            raise ValueError(f"Use null for unknown {key}")
        if key in ENUMS and value not in ENUMS[key]:
            raise ValueError(f"Unsupported {key}: {value}")
        if not isinstance(fact["spans"], list) or not fact["spans"]:
            raise ValueError(f"Source spans required for {key}")
        for span in fact["spans"]:
            validate_span(span, paper)
        values[key] = value
    if values["ingredient_name"] is None or values["measured_endpoint"] is None:
        raise ValueError("An atomic outcome needs an ingredient and measured endpoint")
    validate_span(draft["result_span"], paper)
    # Structural consistency only. Whether the quote ENTAILS this value still
    # needs independent review. No keyword matching silently approves a claim.
    blockers = [f"missing:{key}" for key, value in values.items() if value is None]
    checks = {
        "human_skin_required": values["subject"] == "human_skin",
        "topical_required": values["route"] == "topical",
        "efficacy_required": values["claim_kind"] == "efficacy",
        "ingredient_contribution_unresolved": values["attribution"] == "isolated_ingredient",
        "outcome_control_required": values["result_comparator"] in {"matched_vehicle", "ingredient_add_on_control"},
        "positive_result_required": values["result_support"] == "supported",
        "endpoint_significance_required": values["significance"] == "significant",
    }
    blockers.extend(reason for reason, passed in checks.items() if not passed)
    expected = OUTCOMES.get(values["measured_endpoint"])
    direction_matches = expected is not None and expected[1] == values["change_direction"]
    if not direction_matches:
        blockers.append("endpoint_direction_not_positive_pilot_mapping")
    if paper["corrections"]:
        blockers.append("linked_correction_or_retraction_requires_review")
    # Stable identity excludes annotator notes; revisions still get a separate
    # full draft hash. Multiple comparisons/endpoints cannot overwrite each other.
    identity = {"version": VERSION, "source_sha256": source_hash, "pmid": paper["pmid"],
                "fields": fields, "result_span": draft["result_span"]}
    return {
        "observation_id": record_id(identity), "draft_sha256": record_id(draft),
        "schema_version": VERSION, "status": "pending_independent_review",
        "recommendation_eligible": False,
        "proposed_effect_code": expected[0] if direction_matches and values["result_support"] == "supported" and values["claim_kind"] == "efficacy" else None,
        "pre_review_blockers": blockers, "draft": draft,
        "source": {"pmid": paper["pmid"], "title": paper["title"], "url": paper["source_url"],
                   "response_sha256": source_hash, "corrections": paper["corrections"],
                   "abstract_sections": paper["abstract_sections"]},
    }


def build(snapshot: Path, annotations: Path, output: Path) -> dict:
    if output.exists():
        raise FileExistsError("Use a new isolated review directory")
    manifest, papers = load_snapshot(snapshot)
    annotation_bytes = annotations.read_bytes()
    drafts = [json.loads(line) for line in annotation_bytes.decode("utf-8").splitlines() if line.strip()]
    if not drafts:
        raise ValueError("No observations supplied")
    packets, seen = [], set()
    for draft in drafts:
        if not isinstance(draft, dict) or draft.get("pmid") not in papers:
            raise ValueError("Draft PMID not found in snapshot")
        packet = make_packet(draft, papers[draft["pmid"]], manifest["response_sha256"])
        if packet["observation_id"] in seen:
            raise ValueError("Duplicate atomic observation; do not double-count")
        seen.add(packet["observation_id"])
        packets.append(packet)
    repo = Path(__file__).resolve().parents[1]
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True)
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=repo, capture_output=True, text=True)
    result = {
        "schema_version": VERSION, "created_at": datetime.now(timezone.utc).isoformat(),
        "code_sha": sha.stdout.strip() if sha.returncode == 0 else None,
        "code_dirty": bool(dirty.stdout.strip()) if dirty.returncode == 0 else None,
        "snapshot_manifest_sha256": hashlib.sha256((snapshot / "manifest.json").read_bytes()).hexdigest(),
        "source_response_sha256": manifest["response_sha256"],
        "annotations_sha256": hashlib.sha256(annotation_bytes).hexdigest(),
        "observation_count": len(packets), "observed_pmid_count": len({d["pmid"] for d in drafts}),
        "unannotated_pmids": sorted(set(papers) - {d["pmid"] for d in drafts}),
        "blocker_counts": dict(Counter(reason for p in packets for reason in p["pre_review_blockers"])),
        "recommendation_eligible_count": 0,
        "warning": "Manual provisional interpretations, not independently reviewed or a complete outcome inventory. Spans prove presence, not entailment. No graph export.",
    }
    # Complete validation before creating anything; source files remain untouched.
    output.mkdir(parents=True)
    with (output / "review_packets.jsonl").open("x", encoding="utf-8") as handle:
        for packet in packets:
            handle.write(json.dumps(packet, ensure_ascii=False) + "\n")
    with (output / "manifest.json").open("x", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-dir", required=True, type=Path)
    parser.add_argument("--annotations", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(build(args.snapshot_dir, args.annotations, args.output_dir), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
