import csv
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

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
from pipeline.common.models.gold_record import (
    GoldClaimConcernMapRecord,
    GoldClaimEffectMapRecord,
    GoldClaimRecord,
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


def safe_float(value: Optional[str], default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except ValueError:
        return default


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


def _build_claim_key(
    pmid: str,
    chunk_index: int,
    ingredient: str,
    relation: str,
    target: str,
    source_sentence: str,
) -> str:
    raw = f"{pmid}|{chunk_index}|{ingredient}|{relation}|{target}|{source_sentence}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _normalize_summary(claim: dict) -> str:
    return f'{claim["ingredient"]} {claim["relation"]} {claim["target"]}'


def _is_duplicate_within_batch(
    claim_key: str,
    seen_claim_keys: set[str],
) -> bool:
    if claim_key in seen_claim_keys:
        return True
    seen_claim_keys.add(claim_key)
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


def _map_effects(validated_claim: dict, effect_rows: List[Dict]) -> List[Dict]:
    target = validated_claim["target"].lower()
    matched: List[Dict] = []

    for row in effect_rows:
        effect_name = (row.get("effect_name_en") or "").lower()
        effect_code = (row.get("effect_code") or "").lower()

        if target == effect_name or target == effect_code:
            matched.append(row)
            continue

        if effect_name and effect_name in target:
            matched.append(row)

    unique = {}
    for row in matched:
        unique[row["effect_id"]] = row
    return list(unique.values())


def _map_concerns(validated_claim: dict, concern_rows: List[Dict]) -> List[Dict]:
    target = validated_claim["target"].lower()
    matched: List[Dict] = []

    for row in concern_rows:
        concern_name = (row.get("concern_name_en") or "").lower()
        concern_code = (row.get("concern_code") or "").lower()

        if target == concern_name or target == concern_code:
            matched.append(row)
            continue

        if concern_name and concern_name in target:
            matched.append(row)

    unique = {}
    for row in matched:
        unique[row["concern_id"]] = row
    return list(unique.values())


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

    gold_batch_id = build_batch_id()
    gold_batch_dir = settings.gold_claim_dir / f"batch={gold_batch_id}"
    ensure_dir(gold_batch_dir)

    claim_records: List[GoldClaimRecord] = []
    effect_map_records: List[GoldClaimEffectMapRecord] = []
    concern_map_records: List[GoldClaimConcernMapRecord] = []

    seen_claim_keys: set[str] = set()
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

                claim_key = _build_claim_key(
                    pmid=chunk["pmid"],
                    chunk_index=safe_int(chunk.get("chunk_index")) or 0,
                    ingredient=validated_claim["ingredient"],
                    relation=validated_claim["relation"],
                    target=validated_claim["target"],
                    source_sentence=sentence,
                )

                if _is_duplicate_within_batch(claim_key, seen_claim_keys):
                    continue

                normalized_summary = _normalize_summary(validated_claim)
                confidence = float(validated_claim["confidence"])

                claim_record = GoldClaimRecord(
                    batch_id=gold_batch_id,
                    claim_key=claim_key,
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

                mapped_effects = _map_effects(validated_claim, effect_rows)
                for effect in mapped_effects:
                    effect_map_records.append(
                        GoldClaimEffectMapRecord(
                            batch_id=gold_batch_id,
                            claim_key=claim_key,
                            effect_id=effect["effect_id"],
                            effect_code=effect["effect_code"],
                            effect_name_en=effect["effect_name_en"],
                            confidence_score=min(confidence, 0.7),
                        )
                    )

                mapped_concerns = _map_concerns(validated_claim, concern_rows)
                for concern in mapped_concerns:
                    concern_map_records.append(
                        GoldClaimConcernMapRecord(
                            batch_id=gold_batch_id,
                            claim_key=claim_key,
                            concern_id=concern["concern_id"],
                            concern_code=concern["concern_code"],
                            concern_name_en=concern["concern_name_en"],
                            confidence_score=min(confidence, 0.7),
                        )
                    )

        if chunk_has_candidate:
            candidate_chunk_count += 1

    claim_rows = [record.to_dict() for record in claim_records]
    effect_map_rows = [record.to_dict() for record in effect_map_records]
    concern_map_rows = [record.to_dict() for record in concern_map_records]

    write_csv(gold_batch_dir / "graph_claim.csv", claim_rows)
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