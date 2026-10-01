"""Offline shadow audit. No API calls, uploads, production export or DB writes.

python -m scripts.audit_evidence_gate --claims /absolute/gold_claim_all.csv \
    --output-dir /absolute/new-audit-directory [--reviews reviewed_claims.jsonl]
Run one explicit batch per audit; legacy batches must not be silently combined.
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

from pipeline.gold.claim.evidence_gate import GATE_VERSION, candidate_edge, evaluate, record_id


def load_reviews(path: Path | None) -> dict:
    reviews = {}
    if path is not None:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            review = json.loads(line)
            rid = review.get("record_id")
            if not isinstance(rid, str) or not rid or rid in reviews:
                raise ValueError("Missing or duplicate review record_id")
            reviews[rid] = review
    return reviews


def audit(claims: Path, output: Path, reviews_path: Path | None = None) -> dict:
    source_bytes = claims.read_bytes()
    source_hash = hashlib.sha256(source_bytes).hexdigest()
    reviews = load_reviews(reviews_path)
    rows = list(csv.DictReader(source_bytes.decode("utf-8-sig").splitlines(keepends=True)))
    if not rows:
        raise ValueError("No source rows")
    record_ids = {record_id(row) for row in rows}
    if set(reviews) - record_ids:
        raise ValueError("Review file contains records not present in this exact source batch")
    # Never overwrite an earlier audit or write into a production data directory.
    if output.exists():
        raise FileExistsError(f"Use a new isolated output directory: {output}")
    output.mkdir(parents=True)
    counts, reasons = Counter(), Counter()
    effects = defaultdict(set)
    seen = set()
    with (output / "decisions.jsonl").open("x", encoding="utf-8") as decisions, (
        output / "candidate_edges.jsonl"
    ).open("x", encoding="utf-8") as edges:
        for record_number, row in enumerate(rows, start=1):
            rid = record_id(row)
            if rid in seen:
                continue
            seen.add(rid)
            review = reviews.get(rid)
            decision = evaluate(row, review)
            counts[decision.status] += 1
            reasons.update(decision.reasons)
            item = {
                **decision.to_dict(), "source_csv": str(claims.resolve()),
                "source_csv_sha256": source_hash, "csv_record_number": record_number,
                "source": row, "review": review,
            }
            decisions.write(json.dumps(item, ensure_ascii=False) + "\n")
            if decision.status == "candidate":
                edge = candidate_edge(row, review, decision)
                edge.update(source_csv_sha256=source_hash, csv_record_number=record_number)
                edges.write(json.dumps(edge, ensure_ascii=False) + "\n")
                effects[decision.effect_code].add(row["pmid"])
    repo = Path(__file__).resolve().parents[1]
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True)
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=repo, capture_output=True, text=True)
    manifest = {
        "gate_version": GATE_VERSION, "mode": "offline_shadow_not_production",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "code_sha": sha.stdout.strip() if sha.returncode == 0 else None,
        "code_dirty": bool(dirty.stdout.strip()) if dirty.returncode == 0 else None,
        "source_csv": str(claims.resolve()), "source_csv_sha256": source_hash,
        "reviews_sha256": hashlib.sha256(reviews_path.read_bytes()).hexdigest() if reviews_path else None,
        "input_rows": len(rows), "unique_records": len(seen),
        "duplicate_records": len(rows) - len(seen),
        "status_counts": dict(counts), "reason_counts": dict(reasons),
        "candidate_unique_pmids_by_effect": {k: len(v) for k, v in effects.items()},
        "warning": "Unreviewed is not disproven. Zero candidates without reviews is expected; not a measured error rate.",
    }
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--claims", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--reviews", type=Path)
    args = parser.parse_args()
    print(json.dumps(audit(args.claims, args.output_dir, args.reviews), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
