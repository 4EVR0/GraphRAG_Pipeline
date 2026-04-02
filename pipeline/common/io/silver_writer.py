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


def build_silver_metadata(
    *,
    batch_id: str,
    bronze_batch_id: str,
    raw_paper_count: int,
    deduped_paper_count: int,
    chunk_count: int,
    created_at: str,
    chunk_version: str,
    code_version: str | None = None,
) -> dict:
    return {
        "layer": "silver",
        "domain": "paper",
        "batch_id": batch_id,
        "input_layer": f"bronze/pubmed/batch={bronze_batch_id}",
        "raw_paper_count": raw_paper_count,
        "deduped_paper_count": deduped_paper_count,
        "chunk_count": chunk_count,
        "chunk_version": chunk_version,
        "created_at": created_at,
        "code_version": code_version,
    }