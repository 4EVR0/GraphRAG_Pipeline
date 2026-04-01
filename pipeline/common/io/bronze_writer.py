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


def write_text(path: Path, content: str) -> None:
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def write_json(path: Path, payload: dict) -> None:
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def build_batch_metadata(
    *,
    batch_id: str,
    target_count: int,
    total_search_logs: int,
    total_papers: int,
    created_at: str,
    code_version: str | None = None,
) -> dict:
    return {
        "layer": "bronze",
        "domain": "pubmed",
        "batch_id": batch_id,
        "target_count": target_count,
        "search_log_count": total_search_logs,
        "paper_count": total_papers,
        "created_at": created_at,
        "code_version": code_version,
    }