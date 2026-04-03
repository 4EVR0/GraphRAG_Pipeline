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
    evidence_audit_count: int = 0,
    canonical_claim_count: int = 0,
    graph_eligible_evidence_count: int = 0,
    excluded_evidence_count: int = 0,
    unmapped_target_count: int = 0,
    graph_claim_row_count: int = 0,
    recommendation_claim_row_count: int = 0,
    strict_graph_evidence_count: int = 0,
    soft_graph_evidence_count: int = 0,
    recommendation_only_evidence_count: int = 0,
    evidence_only_count: int = 0,
    single_active_count: int = 0,
    single_formulation_count: int = 0,
    multi_active_combination_count: int = 0,
    procedure_combination_count: int = 0,
    ambiguous_count: int = 0,
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
        "evidence_audit_count": evidence_audit_count,
        "gold_claim_all_row_count": evidence_audit_count,
        "canonical_claim_count": canonical_claim_count,
        "graph_eligible_evidence_count": graph_eligible_evidence_count,
        "excluded_evidence_count": excluded_evidence_count,
        "unmapped_target_count": unmapped_target_count,
        "graph_claim_row_count": graph_claim_row_count,
        "recommendation_claim_row_count": recommendation_claim_row_count,
        "strict_graph_evidence_count": strict_graph_evidence_count,
        "soft_graph_evidence_count": soft_graph_evidence_count,
        "recommendation_only_evidence_count": recommendation_only_evidence_count,
        "evidence_only_count": evidence_only_count,
        "single_active_count": single_active_count,
        "single_formulation_count": single_formulation_count,
        "multi_active_combination_count": multi_active_combination_count,
        "procedure_combination_count": procedure_combination_count,
        "ambiguous_count": ambiguous_count,
        "extractor_version": extractor_version,
        "validator_version": validator_version,
        "mapping_version": mapping_version,
        "created_at": created_at,
        "code_version": code_version,
    }