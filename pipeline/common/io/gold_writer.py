import csv
import json
from pathlib import Path
from typing import Iterable


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, rows: Iterable[dict]) -> int:
    rows = list(rows)
    ensure_dir(path.parent)

    if not rows:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            f.write("")
        return 0

    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


def write_json(path: Path, payload: dict) -> None:
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def build_gold_metadata(
    *,
    batch_id: str,
    silver_batch_id: str,
    chunk_count: int,
    candidate_chunk_count: int,
    total_sentences: int,
    claim_count: int,
    effect_map_count: int,
    concern_map_count: int,
    created_at: str,
    extractor_version: str,
    validator_version: str,
    mapping_version: str,
    code_version: str | None = None,
) -> dict:
    return {
        "layer": "gold",
        "domain": "claim",
        "batch_id": batch_id,
        "input_layer": f"silver/paper/batch={silver_batch_id}",
        "chunk_count": chunk_count,
        "candidate_chunk_count": candidate_chunk_count,
        "total_sentences": total_sentences,
        "claim_count": claim_count,
        "effect_map_count": effect_map_count,
        "concern_map_count": concern_map_count,
        "extractor_version": extractor_version,
        "validator_version": validator_version,
        "mapping_version": mapping_version,
        "created_at": created_at,
        "code_version": code_version,
    }