from pipeline.common.config.settings import settings
from pipeline.common.repositories.paper_repository import get_connection
from pipeline.common.repositories.claim_repository import (
    fetch_unprocessed_chunks,
    fetch_effect_taxonomy,
    fetch_concern_taxonomy,
    get_ingredient_id_by_canonical_name,
    insert_claim,
    insert_claim_ingredient_map,
    insert_claim_effect_map,
    insert_claim_concern_map,
)
from pipeline.claim.services.claim_extractor import extractor


def validate_environment() -> None:
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is not set. Check your .env file.")


def main() -> None:
    validate_environment()
    conn = get_connection(settings.database_url)

    try:
        chunks = fetch_unprocessed_chunks(conn)
        effect_rows = fetch_effect_taxonomy(conn)
        concern_rows = fetch_concern_taxonomy(conn)

        print(f"[INFO] Found {len(chunks)} unprocessed chunks")

        processed_count = 0

        for chunk in chunks:
            try:
                extracted = extractor.extract_claim(
                    chunk_text=chunk["chunk_text"],
                    effect_rows=effect_rows,
                    concern_rows=concern_rows,
                )

                claim_row = {
                    "paper_id": chunk["paper_id"],
                    "chunk_id": chunk["chunk_id"],
                    "claim_text": extracted["claim_text"],
                    "normalized_summary": extracted["normalized_summary"],
                    "claim_type": extracted["claim_type"],
                    "evidence_direction": extracted["evidence_direction"],
                    "confidence_score": extracted["confidence_score"],
                    "section_type": chunk["section_type"],
                    "extraction_method": "rule",
                    "source_sentence": chunk["chunk_text"],
                    "source_start_offset": chunk["source_start_offset"],
                    "source_end_offset": chunk["source_end_offset"],
                }

                claim_id = insert_claim(conn, claim_row)

                for canonical_name in extracted["ingredient_names"]:
                    ingredient_id = get_ingredient_id_by_canonical_name(conn, canonical_name)
                    if ingredient_id is not None:
                        insert_claim_ingredient_map(conn, claim_id, ingredient_id)

                for effect_id in extracted["effect_ids"]:
                    insert_claim_effect_map(conn, claim_id, effect_id)

                for concern_id in extracted["concern_ids"]:
                    insert_claim_concern_map(conn, claim_id, concern_id)

                conn.commit()
                processed_count += 1

                if processed_count % 50 == 0:
                    print(f"[INFO] Processed {processed_count} chunks")

            except Exception as exc:
                conn.rollback()
                print(f"[ERROR] Failed chunk_id={chunk['chunk_id']}: {exc}")

        print(f"[INFO] Completed claim extraction for {processed_count} chunks")

    finally:
        conn.close()


if __name__ == "__main__":
    main()