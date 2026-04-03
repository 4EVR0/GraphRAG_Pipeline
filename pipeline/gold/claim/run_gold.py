import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from pipeline.claim.services.claim_extractor import extractor
from pipeline.claim.services.claim_filter import (
    is_blocked_sentence,
    is_claim_candidate_sentence,
    is_claim_worthy_section,
)
from pipeline.claim.services.llm_claim_extractor import llm_extractor
from pipeline.claim.services.sentence_splitter import split_sentences
from pipeline.common.config.settings import settings
from pipeline.common.io.gold_writer import (
    build_gold_metadata,
    ensure_dir,
    write_csv,
    write_json,
)
from pipeline.common.models.gold_record import GoldClaimConcernMapRecord, GoldClaimEffectMapRecord, GoldClaimRecord
from pipeline.gold.claim.evidence_scoring import (
    aggregate_canonical_rows,
    assert_canonical_score_order,
    assert_tier_valid,
    build_canonical_claim_key,
    build_dedup_scope_key,
    build_evidence_id,
    build_policy_reasons,
    compute_eligibility_tier,
    compute_row_weight,
    is_generalized_review_style,
    is_graph_eligible_tier,
    label_attribution_v2,
    label_modality,
    label_significance_v2,
    label_strength_v2,
    list_detected_ingredients_in_sentence,
)

try:
    from pipeline.common.repositories.paper_repository import get_connection
    from pipeline.common.repositories.claim_repository import (
        fetch_effect_taxonomy,
        fetch_concern_taxonomy,
        get_ingredient_id_by_canonical_name,
        insert_claim,
        insert_claim_ingredient_map,
        insert_claim_effect_map,
        insert_claim_concern_map,
    )
except Exception:
    get_connection = None
    fetch_effect_taxonomy = None
    fetch_concern_taxonomy = None
    get_ingredient_id_by_canonical_name = None
    insert_claim = None
    insert_claim_ingredient_map = None
    insert_claim_effect_map = None
    insert_claim_concern_map = None


def build_batch_id() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%dT%H-%M-%S")


def resolve_silver_batch_dir(silver_batch_id: Optional[str]) -> Path:
    if silver_batch_id:
        batch_dir = settings.silver_paper_dir / f"batch={silver_batch_id}"
        if not batch_dir.exists():
            raise FileNotFoundError(f"Silver batch not found: {batch_dir}")
        return batch_dir

    batch_dirs = sorted(
        [p for p in settings.silver_paper_dir.glob("batch=*") if p.is_dir()],
        reverse=True,
    )
    if not batch_dirs:
        raise FileNotFoundError("No silver batch directories found.")

    return batch_dirs[0]


def read_silver_chunks(csv_path: Path) -> List[dict]:
    if not csv_path.exists():
        raise FileNotFoundError(f"paper_chunk.csv not found: {csv_path}")

    rows: List[dict] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def safe_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _get_sentence_level_ingredient_candidates(sentence: str) -> list[str]:
    sentence = sentence.strip()
    if not sentence:
        return []
    return extractor.extract_ingredient_names(sentence)


def _get_chunk_level_ingredient_candidates(sentences: list[str]) -> list[str]:
    candidates: list[str] = []

    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue

        sentence_candidates = _get_sentence_level_ingredient_candidates(sentence)
        if sentence_candidates:
            candidates.extend(sentence_candidates)

    return list(dict.fromkeys(candidates))


def _resolve_ingredient_candidates_for_sentence(
    sentence: str,
    chunk_ingredient_candidates: list[str],
) -> list[str]:
    sentence_candidates = _get_sentence_level_ingredient_candidates(sentence)
    if sentence_candidates:
        return sentence_candidates

    if len(chunk_ingredient_candidates) == 1:
        return chunk_ingredient_candidates

    return []


def _validate_claim_compat(raw_claim: dict, sentence: str) -> dict | None:
    validate_fn = extractor.validate_claim

    try:
        return validate_fn(raw_claim, source_sentence=sentence)
    except TypeError:
        return validate_fn(raw_claim)


def _normalize_summary(claim: dict) -> str:
    return f'{claim["ingredient"]} {claim["relation"]} {claim["target"]}'


def _dedup_seen(dedup_scope_key: str, seen: Set[str]) -> bool:
    if dedup_scope_key in seen:
        return True
    seen.add(dedup_scope_key)
    return False


def _fetch_taxonomy_rows() -> tuple[List[Dict], List[Dict]]:
    if not settings.database_url:
        return [], []

    if not get_connection or not fetch_effect_taxonomy or not fetch_concern_taxonomy:
        return [], []

    conn = get_connection(settings.database_url)
    try:
        effect_rows = fetch_effect_taxonomy(conn)
        concern_rows = fetch_concern_taxonomy(conn)
        return effect_rows, concern_rows
    finally:
        conn.close()


def maybe_upsert_claims_to_db(
    claim_rows: List[dict],
    effect_map_rows: List[dict],
    concern_map_rows: List[dict],
) -> None:
    if not settings.enable_claim_db_upsert:
        return

    required_imports = [
        get_connection,
        get_ingredient_id_by_canonical_name,
        insert_claim,
        insert_claim_ingredient_map,
        insert_claim_effect_map,
        insert_claim_concern_map,
    ]
    if any(fn is None for fn in required_imports):
        raise RuntimeError("Claim DB upsert is enabled, but repository import failed.")

    if not settings.database_url:
        raise RuntimeError("ENABLE_CLAIM_DB_UPSERT=true but DATABASE_URL is not set.")

    effect_map_by_claim = {}
    for row in effect_map_rows:
        effect_map_by_claim.setdefault(row["claim_key"], []).append(row)

    concern_map_by_claim = {}
    for row in concern_map_rows:
        concern_map_by_claim.setdefault(row["claim_key"], []).append(row)

    conn = get_connection(settings.database_url)
    try:
        inserted = 0

        for row in claim_rows:
            ingredient_id = get_ingredient_id_by_canonical_name(conn, row["ingredient_name"])
            if ingredient_id is None:
                continue

            claim_row = {
                "paper_id": None,
                "chunk_id": None,
                "claim_text": row["claim_text"],
                "normalized_summary": row["normalized_summary"],
                "claim_type": row["claim_type"],
                "evidence_direction": row["evidence_direction"],
                "confidence_score": row["confidence_score"],
                "section_type": row["section_type"],
                "extraction_method": row["extraction_method"],
                "source_sentence": row["source_sentence"],
                "source_start_offset": row["source_start_offset"],
                "source_end_offset": row["source_end_offset"],
            }

            claim_id = insert_claim(conn, claim_row)
            insert_claim_ingredient_map(
                conn,
                claim_id=claim_id,
                ingredient_id=ingredient_id,
                confidence_score=row["confidence_score"],
            )

            for effect_row in effect_map_by_claim.get(row["claim_key"], []):
                insert_claim_effect_map(
                    conn,
                    claim_id=claim_id,
                    effect_id=effect_row["effect_id"],
                    confidence_score=effect_row["confidence_score"],
                )

            for concern_row in concern_map_by_claim.get(row["claim_key"], []):
                insert_claim_concern_map(
                    conn,
                    claim_id=claim_id,
                    concern_id=concern_row["concern_id"],
                    confidence_score=concern_row["confidence_score"],
                )

            inserted += 1

        conn.commit()
        print(f"[INFO] Inserted {inserted} claims into extracted_claim and map tables")
    finally:
        conn.close()


def main(silver_batch_id: Optional[str] = None) -> None:
    silver_batch_dir = resolve_silver_batch_dir(silver_batch_id)
    silver_batch_name = silver_batch_dir.name
    silver_batch_id = silver_batch_name.replace("batch=", "")

    chunk_csv_path = silver_batch_dir / "paper_chunk.csv"
    chunks = read_silver_chunks(chunk_csv_path)
    print(f"[INFO] Loaded {len(chunks)} chunk rows from {chunk_csv_path}")

    if settings.gold_test_chunk_limit > 0:
        chunks = chunks[: settings.gold_test_chunk_limit]

    effect_rows, concern_rows = _fetch_taxonomy_rows()
    effect_by_id: Dict[int, Dict] = {row["effect_id"]: row for row in effect_rows}
    concern_by_id: Dict[int, Dict] = {row["concern_id"]: row for row in concern_rows}

    allowed_canonical_ingredients = extractor.get_allowed_ingredient_names()

    gold_batch_id = build_batch_id()
    gold_batch_dir = settings.gold_claim_dir / f"batch={gold_batch_id}"
    ensure_dir(gold_batch_dir)

    claim_records: List[GoldClaimRecord] = []
    effect_map_records: List[GoldClaimEffectMapRecord] = []
    concern_map_records: List[GoldClaimConcernMapRecord] = []

    evidence_for_aggregate: List[Dict[str, Any]] = []
    audit_rows: List[Dict[str, Any]] = []
    full_claim_export_rows: List[Dict[str, Any]] = []
    seen_dedup_scope: Set[str] = set()
    total_sentences = 0
    candidate_chunk_count = 0

    for chunk in chunks:
        chunk_text = (chunk.get("chunk_text") or "").strip()
        section_type = chunk.get("section_type") or "abstract"
        if not chunk_text:
            continue

        if not is_claim_worthy_section(section_type):
            continue

        sentences = split_sentences(chunk_text)
        if not sentences:
            continue

        chunk_has_candidate = False
        chunk_ingredient_candidates = _get_chunk_level_ingredient_candidates(sentences)

        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue

            total_sentences += 1

            if is_blocked_sentence(sentence):
                continue

            if not is_claim_candidate_sentence(sentence):
                continue

            ingredient_candidates = _resolve_ingredient_candidates_for_sentence(
                sentence=sentence,
                chunk_ingredient_candidates=chunk_ingredient_candidates,
            )
            if not ingredient_candidates:
                continue

            chunk_has_candidate = True

            raw_claims = llm_extractor.extract(
                sentence=sentence,
                ingredient_candidates=ingredient_candidates,
            )
            if not raw_claims:
                continue

            for raw_claim in raw_claims:
                validated_claim = _validate_claim_compat(raw_claim=raw_claim, sentence=sentence)
                if validated_claim is None:
                    continue

                dedup_scope_key = build_dedup_scope_key(
                    pmid=chunk["pmid"],
                    source_sentence=sentence,
                    ingredient_name=validated_claim["ingredient"],
                    relation=validated_claim["relation"],
                    target=validated_claim["target"],
                )
                if _dedup_seen(dedup_scope_key, seen_dedup_scope):
                    continue

                evidence_id = build_evidence_id(gold_batch_id, dedup_scope_key)
                canonical_claim_key = build_canonical_claim_key(
                    validated_claim["ingredient"],
                    validated_claim["relation"],
                    validated_claim["target"],
                    validated_claim["target_category"],
                )

                hedging = bool(raw_claim.get("hedging", False))
                study_context = str(raw_claim.get("study_context") or "unknown").strip() or "unknown"
                chunk_title = (chunk.get("title") or "").strip()

                normalized_summary = _normalize_summary(validated_claim)
                all_detected = list_detected_ingredients_in_sentence(
                    sentence, allowed_canonical_ingredients
                )
                all_detected_str = "|".join(all_detected)

                significance_label = label_significance_v2(
                    sentence,
                    validated_claim["claim_type"],
                    validated_claim["relation"],
                    target=validated_claim["target"],
                )
                strength_label = label_strength_v2(sentence, hedging, significance_label)
                attribution_label = label_attribution_v2(
                    sentence,
                    validated_claim["ingredient"],
                    allowed_canonical_ingredients,
                    normalized_summary=normalized_summary,
                    section_type=section_type,
                    title=chunk_title,
                )
                modality_label = label_modality(
                    validated_claim["claim_type"],
                    validated_claim["relation"],
                    sentence,
                )

                confidence = float(validated_claim["confidence"])

                taxonomy_maps = extractor.infer_taxonomy_maps(
                    validated_claim=validated_claim,
                    effect_rows=effect_rows,
                    concern_rows=concern_rows,
                )
                effect_ids_list = list(taxonomy_maps.get("effect_ids") or [])
                concern_ids_list = list(taxonomy_maps.get("concern_ids") or [])

                eligibility_tier = compute_eligibility_tier(
                    strength_label,
                    significance_label,
                    attribution_label,
                    validated_claim["claim_type"],
                    effect_ids_list,
                    concern_ids_list,
                    sentence=sentence,
                    title=chunk_title,
                    study_context=study_context,
                )
                assert_tier_valid(eligibility_tier)
                is_graph_eligible = is_graph_eligible_tier(eligibility_tier)
                row_weight = compute_row_weight(
                    strength_label,
                    significance_label,
                    attribution_label,
                    study_context,
                )

                has_mapping = bool(effect_ids_list or concern_ids_list)
                is_review = is_generalized_review_style(sentence, chunk_title, study_context)
                exclusion_reason, recommendation_reason = build_policy_reasons(
                    eligibility_tier,
                    attribution_label,
                    strength_label,
                    significance_label,
                    has_mapping,
                    is_review,
                )

                effect_ids_str = "|".join(str(i) for i in sorted(set(effect_ids_list)))
                concern_ids_str = "|".join(str(i) for i in sorted(set(concern_ids_list)))

                evidence_for_aggregate.append(
                    {
                        "canonical_claim_key": canonical_claim_key,
                        "pmid": chunk["pmid"],
                        "row_weight": row_weight,
                        "is_graph_eligible": is_graph_eligible,
                        "eligibility_tier": eligibility_tier,
                        "attribution_label": attribution_label,
                        "ingredient_name": validated_claim["ingredient"],
                        "relation": validated_claim["relation"],
                        "target": validated_claim["target"],
                        "target_category": validated_claim["target_category"],
                        "effect_ids_list": effect_ids_list,
                        "concern_ids_list": concern_ids_list,
                        "study_context": study_context,
                    }
                )

                audit_rows.append(
                    {
                        "evidence_id": evidence_id,
                        "batch_id": gold_batch_id,
                        "pmid": chunk["pmid"],
                        "title": chunk_title,
                        "journal": (chunk.get("journal") or "").strip(),
                        "publication_year": safe_int(chunk.get("publication_year")) or "",
                        "section_type": section_type,
                        "chunk_index": safe_int(chunk.get("chunk_index")) or 0,
                        "source_sentence": sentence,
                        "ingredient_name": validated_claim["ingredient"],
                        "relation": validated_claim["relation"],
                        "target": validated_claim["target"],
                        "target_category": validated_claim["target_category"],
                        "normalized_summary": normalized_summary,
                        "all_detected_ingredients": all_detected_str,
                        "effect_ids": effect_ids_str,
                        "concern_ids": concern_ids_str,
                        "canonical_claim_key": canonical_claim_key,
                        "strength_label": strength_label,
                        "significance_label": significance_label,
                        "attribution_label": attribution_label,
                        "modality_label": modality_label,
                        "dedup_scope_key": dedup_scope_key,
                        "eligibility_tier": eligibility_tier,
                        "is_graph_eligible": is_graph_eligible,
                        "row_weight": row_weight,
                        "study_context": study_context,
                        "confidence_score": confidence,
                        "claim_type": validated_claim["claim_type"],
                        "evidence_direction": validated_claim["evidence_direction"],
                        "exclusion_reason": exclusion_reason,
                        "recommendation_reason": recommendation_reason,
                    }
                )

                claim_record = GoldClaimRecord(
                    batch_id=gold_batch_id,
                    claim_key=evidence_id,
                    pmid=chunk["pmid"],
                    chunk_index=safe_int(chunk.get("chunk_index")) or 0,
                    section_type=section_type,
                    source_sentence=sentence,
                    ingredient_name=validated_claim["ingredient"],
                    claim_text=sentence,
                    normalized_summary=normalized_summary,
                    claim_type=validated_claim["claim_type"],
                    relation=validated_claim["relation"],
                    target=validated_claim["target"],
                    target_category=validated_claim["target_category"],
                    evidence_direction=validated_claim["evidence_direction"],
                    confidence_score=confidence,
                    extraction_method="llm_sentence_extraction",
                    extractor_version=settings.extractor_version,
                    validator_version=settings.validator_version,
                    mapping_version=settings.mapping_version,
                    source_start_offset=safe_int(chunk.get("source_start_offset")) or 0,
                    source_end_offset=safe_int(chunk.get("source_end_offset")) or 0,
                    title=(chunk.get("title") or "").strip() or None,
                    journal=(chunk.get("journal") or "").strip() or None,
                    publication_year=safe_int(chunk.get("publication_year")),
                    source_url=(chunk.get("source_url") or "").strip() or None,
                )
                claim_records.append(claim_record)

                export_row = claim_record.to_dict()
                export_row["eligibility_tier"] = eligibility_tier
                export_row["all_detected_ingredients"] = all_detected_str
                full_claim_export_rows.append(export_row)

                for effect_id in effect_ids_list:
                    row = effect_by_id.get(effect_id)
                    if not row:
                        continue
                    effect_map_records.append(
                        GoldClaimEffectMapRecord(
                            batch_id=gold_batch_id,
                            claim_key=evidence_id,
                            effect_id=row["effect_id"],
                            effect_code=row["effect_code"],
                            effect_name_en=row["effect_name_en"],
                            confidence_score=min(max(confidence, 0.6), 0.85),
                        )
                    )

                for concern_id in concern_ids_list:
                    row = concern_by_id.get(concern_id)
                    if not row:
                        continue
                    concern_map_records.append(
                        GoldClaimConcernMapRecord(
                            batch_id=gold_batch_id,
                            claim_key=evidence_id,
                            concern_id=row["concern_id"],
                            concern_code=row["concern_code"],
                            concern_name_en=row["concern_name_en"],
                            confidence_score=min(max(confidence, 0.6), 0.85),
                        )
                    )

        if chunk_has_candidate:
            candidate_chunk_count += 1

    claim_rows = [record.to_dict() for record in claim_records]
    effect_map_rows = [record.to_dict() for record in effect_map_records]
    concern_map_rows = [record.to_dict() for record in concern_map_records]

    canonical_rows = aggregate_canonical_rows(gold_batch_id, evidence_for_aggregate)
    if canonical_rows:
        assert_canonical_score_order(canonical_rows)

    graph_claim_rows = [
        r
        for r in full_claim_export_rows
        if r.get("eligibility_tier") in ("strict_graph", "soft_graph")
    ]
    recommendation_claim_rows = [
        r
        for r in full_claim_export_rows
        if r.get("eligibility_tier")
        in ("strict_graph", "soft_graph", "recommendation_only")
    ]
    for r in graph_claim_rows:
        if r.get("eligibility_tier") not in ("strict_graph", "soft_graph"):
            raise RuntimeError(
                "graph_claim.csv purity violated: row not strict_graph/soft_graph"
            )
    if len(recommendation_claim_rows) < len(graph_claim_rows):
        raise RuntimeError(
            "recommendation_claim must be a superset of graph_claim (v3 invariant)"
        )

    unmapped_rows: List[Dict[str, Any]] = []
    excluded_rows: List[Dict[str, Any]] = []
    graph_eligible_count = 0
    tier_counts: Dict[str, int] = {
        "strict_graph": 0,
        "soft_graph": 0,
        "recommendation_only": 0,
        "evidence_only": 0,
    }
    attr_counts: Dict[str, int] = {
        "single_active": 0,
        "single_formulation": 0,
        "multi_active_combination": 0,
        "procedure_combination": 0,
        "ambiguous": 0,
    }

    for row in audit_rows:
        tier = row.get("eligibility_tier") or "evidence_only"
        if tier in tier_counts:
            tier_counts[tier] += 1
        al = row.get("attribution_label") or ""
        if al in attr_counts:
            attr_counts[al] += 1
        if row["is_graph_eligible"]:
            graph_eligible_count += 1
        if not row["effect_ids"] and not row["concern_ids"]:
            unmapped_rows.append(
                {
                    "evidence_id": row["evidence_id"],
                    "batch_id": row["batch_id"],
                    "pmid": row["pmid"],
                    "source_sentence": row["source_sentence"],
                    "ingredient_name": row["ingredient_name"],
                    "target": row["target"],
                    "target_category": row["target_category"],
                    "normalized_summary": row["normalized_summary"],
                    "reason": "no_effect_and_no_concern_mapping",
                }
            )
        if row.get("eligibility_tier") == "evidence_only":
            excluded_rows.append(dict(row))

    if len(claim_rows) != len(audit_rows):
        raise RuntimeError(
            f"metadata consistency: claim_count {len(claim_rows)} != gold_claim_all {len(audit_rows)}"
        )
    if graph_eligible_count != tier_counts["strict_graph"] + tier_counts["soft_graph"]:
        raise RuntimeError(
            "graph_eligible_evidence_count must equal strict_graph + soft_graph (v3)"
        )

    write_csv(gold_batch_dir / "gold_claim_all.csv", audit_rows)
    write_csv(gold_batch_dir / "gold_canonical_claim.csv", canonical_rows)
    write_csv(gold_batch_dir / "gold_excluded_claims.csv", excluded_rows)
    write_csv(gold_batch_dir / "gold_unmapped_targets.csv", unmapped_rows)

    write_csv(gold_batch_dir / "graph_claim.csv", graph_claim_rows)
    write_csv(gold_batch_dir / "recommendation_claim.csv", recommendation_claim_rows)
    write_csv(gold_batch_dir / "claim_effect_map.csv", effect_map_rows)
    write_csv(gold_batch_dir / "claim_concern_map.csv", concern_map_rows)

    metadata = build_gold_metadata(
        batch_id=gold_batch_id,
        silver_batch_id=silver_batch_id,
        chunk_count=len(chunks),
        candidate_chunk_count=candidate_chunk_count,
        total_sentences=total_sentences,
        claim_count=len(claim_rows),
        effect_map_count=len(effect_map_rows),
        concern_map_count=len(concern_map_rows),
        created_at=datetime.now().astimezone().isoformat(),
        extractor_version=settings.extractor_version,
        validator_version=settings.validator_version,
        mapping_version=settings.mapping_version,
        code_version=None,
        evidence_audit_count=len(audit_rows),
        canonical_claim_count=len(canonical_rows),
        graph_eligible_evidence_count=graph_eligible_count,
        excluded_evidence_count=len(excluded_rows),
        unmapped_target_count=len(unmapped_rows),
        graph_claim_row_count=len(graph_claim_rows),
        recommendation_claim_row_count=len(recommendation_claim_rows),
        strict_graph_evidence_count=tier_counts["strict_graph"],
        soft_graph_evidence_count=tier_counts["soft_graph"],
        recommendation_only_evidence_count=tier_counts["recommendation_only"],
        evidence_only_count=tier_counts["evidence_only"],
        single_active_count=attr_counts["single_active"],
        single_formulation_count=attr_counts["single_formulation"],
        multi_active_combination_count=attr_counts["multi_active_combination"],
        procedure_combination_count=attr_counts["procedure_combination"],
        ambiguous_count=attr_counts["ambiguous"],
    )
    write_json(gold_batch_dir / "metadata.json", metadata)

    maybe_upsert_claims_to_db(
        claim_rows=claim_rows,
        effect_map_rows=effect_map_rows,
        concern_map_rows=concern_map_rows,
    )

    print(f"[INFO] Gold batch saved to: {gold_batch_dir}")


if __name__ == "__main__":
    main()