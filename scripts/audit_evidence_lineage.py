"""Compare source-bound outcome drafts with ONE explicit legacy Gold batch.

A shared PMID is a search aid, never proof of claim equivalence or graph origin.
No ingredient-name aliasing, approvals, score changes, or database access.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from pipeline.gold.claim.evidence_gate import evaluate, record_id
from scripts.build_evidence_review_packets import load_snapshot, make_packet

VERSION = "evidence-lineage-audit-v1"


def locate_sentence(sentence: str, paper: dict) -> list[dict]:
    """Literal-only locations. Strip only this section's recorded label prefix.

    No fuzzy matching or whitespace normalization: a miss is review work, not
    fabricated provenance. Even an exact sentence says nothing about its meaning.
    """
    locations = []
    if not sentence.strip():
        return locations
    for index, section in enumerate(paper["abstract_sections"]):
        quote = sentence
        prefix = section["label"] + ": "
        stripped_label = False
        if section["label"] and quote.startswith(prefix):
            quote = quote[len(prefix):]
            stripped_label = True
        if quote.strip() and quote in section["text"]:
            locations.append({"section": index, "quote": quote, "section_label_removed": stripped_label})
    return locations


def audit(claims: Path, snapshot: Path, annotations: Path, output: Path) -> dict:
    if output.exists():
        raise FileExistsError("Use a new isolated lineage directory")
    raw = claims.read_bytes()
    rows = list(csv.DictReader(raw.decode("utf-8-sig").splitlines(keepends=True)))
    required = {"pmid", "ingredient_name", "source_sentence", "title", "evidence_id"}
    if not rows or not required.issubset(rows[0]):
        raise ValueError("Missing legacy claim source columns")
    batches = set()
    unique = {}
    for number, row in enumerate(rows, 1):
        batch = row.get("gold_batch_id") or row.get("batch_id")
        if not batch:
            raise ValueError("Every legacy row must declare a batch")
        if row.get("gold_batch_id") and row.get("batch_id") and row["gold_batch_id"] != row["batch_id"]:
            raise ValueError("Conflicting batch identities")
        batches.add(batch)
        rid = record_id(row)
        if rid not in unique:
            unique[rid] = {"record_id": rid, "csv_record_numbers": [], "source": row}
        unique[rid]["csv_record_numbers"].append(number)
    if len(batches) != 1:
        raise ValueError("Audit one explicit Gold batch at a time")
    snapshot_manifest, papers = load_snapshot(snapshot)
    source_hash = snapshot_manifest["response_sha256"]
    annotation_bytes = annotations.read_bytes()
    drafts = [json.loads(line) for line in annotation_bytes.decode("utf-8").splitlines() if line.strip()]
    if not drafts:
        raise ValueError("No observations supplied")
    observations, seen = defaultdict(list), set()
    for draft in drafts:
        if not isinstance(draft, dict) or draft.get("pmid") not in papers:
            raise ValueError("Draft PMID not present in snapshot")
        packet = make_packet(draft, papers[draft["pmid"]], source_hash)
        if packet["observation_id"] in seen:
            raise ValueError("Duplicate observation")
        seen.add(packet["observation_id"])
        observations[draft["pmid"]].append(packet)
    by_pmid = defaultdict(list)
    for entry in unique.values():
        by_pmid[entry["source"]["pmid"]].append(entry)
    records, counts = [], Counter()
    for pmid, packets in observations.items():
        paper = papers[pmid]
        related = []
        for entry in by_pmid[pmid]:
            locations = locate_sentence(entry["source"]["source_sentence"], paper)
            overlap = []
            for packet in packets:
                result = packet["draft"]["result_span"]
                if any(loc["section"] == result["section"] and
                       (loc["quote"] in result["quote"] or result["quote"] in loc["quote"])
                       for loc in locations):
                    overlap.append(packet["observation_id"])
            related.append({**entry, "literal_source_locations": locations,
                            "literal_result_overlap_observation_ids": overlap,
                            "gate_without_review": evaluate(entry["source"]).to_dict(),
                            "link_status": "same_pmid_only_not_verified_claim_equivalence"})
            counts["legacy_rows_with_literal_source" if locations else "legacy_rows_requiring_source_location_review"] += 1
        status = "same_pmid_rows_found" if related else "absent_from_selected_batch"
        counts[status] += 1
        records.append({
            "pmid": pmid, "status": status, "observations": packets, "legacy_claims": related,
            "production_graph_lineage_verified": False, "recommendation_eligible": False,
        })
    repo = Path(__file__).resolve().parents[1]
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True)
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=repo, capture_output=True, text=True)
    result = {
        "version": VERSION, "created_at": datetime.now(timezone.utc).isoformat(),
        "code_sha": sha.stdout.strip() if sha.returncode == 0 else None,
        "code_dirty": bool(dirty.stdout.strip()) if dirty.returncode == 0 else None,
        "claims_path": str(claims.resolve()), "claims_sha256": hashlib.sha256(raw).hexdigest(),
        "batch_id": next(iter(batches)), "input_rows": len(rows), "unique_records": len(unique),
        "duplicate_records": len(rows) - len(unique),
        "source_response_sha256": source_hash,
        "snapshot_manifest_sha256": hashlib.sha256((snapshot / "manifest.json").read_bytes()).hexdigest(),
        "annotations_sha256": hashlib.sha256(annotation_bytes).hexdigest(),
        "observation_count": len(seen), "observed_pmid_count": len(observations),
        "matched_unique_legacy_rows": sum(len(item["legacy_claims"]) for item in records),
        "counts": dict(counts), "verified_claim_links": 0, "production_graph_lineage_verified": False,
        "warning": "PMID/literal text matches are review leads, not semantic equivalence or production graph provenance. Absence applies only to this selected batch.",
    }
    output.mkdir(parents=True)
    with (output / "lineage_review.jsonl").open("x", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    with (output / "manifest.json").open("x", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--claims", type=Path, required=True)
    parser.add_argument("--snapshot-dir", type=Path, required=True)
    parser.add_argument("--annotations", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(audit(args.claims, args.snapshot_dir, args.annotations, args.output_dir), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
