from __future__ import annotations

import inspect

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
from pipeline.claim.services.claim_filter import (
    is_blocked_sentence,
    is_claim_candidate_sentence,
    is_claim_worthy_section,
)
from pipeline.claim.services.llm_claim_extractor import llm_extractor
from pipeline.claim.services.sentence_splitter import split_sentences


DEBUG_PRINT_LIMIT = 10
TEST_CHUNK_LIMIT = 50


def validate_environment() -> None:
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is not set. Check your .env file.")


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

    # 순서 유지 + 중복 제거
    return list(dict.fromkeys(candidates))


def _chunk_has_candidate_sentence(chunk: dict) -> bool:
    chunk_text = chunk.get("chunk_text", "")
    if not chunk_text:
        return False

    section_type = chunk.get("section_type")
    if not is_claim_worthy_section(section_type):
        return False

    sentences = split_sentences(chunk_text)
    if not sentences:
        return False

    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue

        if is_blocked_sentence(sentence):
            continue

        if not is_claim_candidate_sentence(sentence):
            continue

        sentence_ingredient_candidates = _get_sentence_level_ingredient_candidates(sentence)
        if sentence_ingredient_candidates:
            return True

    return False


def select_candidate_rich_chunks(conn) -> list[dict]:
    """
    전체 unprocessed chunk 중에서,
    문장 단위로 봤을 때 claim candidate + ingredient candidate가
    하나라도 있는 chunk만 추려서 앞 50개 사용.
    """
    all_chunks = fetch_unprocessed_chunks(conn)

    candidate_rich_chunks = [
        chunk
        for chunk in all_chunks
        if _chunk_has_candidate_sentence(chunk)
    ]

    return candidate_rich_chunks[:TEST_CHUNK_LIMIT]


def _validate_claim_compat(raw_claim: dict, sentence: str) -> dict | None:
    """
    claim_extractor.py가
    - 신버전: validate_claim(raw_claim, source_sentence=...)
    - 구버전: validate_claim(raw_claim)
    둘 중 어느 형태여도 동작하게 맞춘다.
    """
    validate_fn = extractor.validate_claim

    try:
        sig = inspect.signature(validate_fn)
        if "source_sentence" in sig.parameters:
            return validate_fn(raw_claim, source_sentence=sentence)
        return validate_fn(raw_claim)
    except TypeError:
        # 예외적으로 signature 확인이 꼬이더라도 fallback
        try:
            return validate_fn(raw_claim, source_sentence=sentence)
        except TypeError:
            return validate_fn(raw_claim)


def main() -> None:
    validate_environment()
    conn = get_connection(settings.database_url)

    try:
        chunks = select_candidate_rich_chunks(conn)
        effect_rows = fetch_effect_taxonomy(conn)
        concern_rows = fetch_concern_taxonomy(conn)

        print(f"[INFO] Found {len(chunks)} candidate-rich unprocessed chunks")

        processed_chunks = 0
        inserted_claims = 0

        total_sentences = 0
        blocked_sentence_count = 0
        non_claim_sentence_count = 0
        no_ingredient_candidate_count = 0
        llm_empty_count = 0
        validation_fail_count = 0

        debug_chunk_printed = 0
        debug_non_claim_printed = 0
        debug_no_ingredient_printed = 0
        debug_llm_empty_printed = 0
        debug_validation_fail_printed = 0
        debug_inserted_printed = 0

        for chunk in chunks:
            try:
                chunk_text = chunk.get("chunk_text", "")
                sentences = split_sentences(chunk_text)

                if not sentences:
                    continue

                section_type = chunk.get("section_type")
                if not is_claim_worthy_section(section_type):
                    blocked_sentence_count += len(sentences)
                    continue

                chunk_ingredient_candidates = _get_chunk_level_ingredient_candidates(sentences)

                if debug_chunk_printed < DEBUG_PRINT_LIMIT:
                    print(
                        f"[DEBUG][CHUNK] "
                        f"chunk_id={chunk['chunk_id']} "
                        f"paper_id={chunk['paper_id']} "
                        f"chunk_ingredients={chunk_ingredient_candidates} "
                        f"text={chunk_text[:250]}"
                    )
                    debug_chunk_printed += 1

                for sentence in sentences:
                    sentence = sentence.strip()
                    if not sentence:
                        continue

                    total_sentences += 1

                    if is_blocked_sentence(sentence):
                        blocked_sentence_count += 1
                        continue

                    if not is_claim_candidate_sentence(sentence):
                        non_claim_sentence_count += 1

                        if debug_non_claim_printed < DEBUG_PRINT_LIMIT:
                            print(
                                f"[DEBUG][NON_CLAIM] "
                                f"chunk_id={chunk['chunk_id']} "
                                f"paper_id={chunk['paper_id']} "
                                f"sentence={sentence}"
                            )
                            debug_non_claim_printed += 1

                        continue

                    sentence_ingredient_candidates = _get_sentence_level_ingredient_candidates(sentence)

                    # 우선 sentence 기준을 사용하되, 비어 있으면 같은 chunk 안의 다른 문장에서 모은 후보를 fallback
                    ingredient_candidates = (
                        sentence_ingredient_candidates or chunk_ingredient_candidates
                    )

                    if not ingredient_candidates:
                        no_ingredient_candidate_count += 1

                        if debug_no_ingredient_printed < DEBUG_PRINT_LIMIT:
                            print(
                                f"[DEBUG][NO_INGREDIENT] "
                                f"chunk_id={chunk['chunk_id']} "
                                f"paper_id={chunk['paper_id']} "
                                f"sentence={sentence}"
                            )
                            debug_no_ingredient_printed += 1

                        continue

                    raw_claims = llm_extractor.extract(
                        sentence=sentence,
                        ingredient_candidates=ingredient_candidates,
                    )

                    if not raw_claims:
                        llm_empty_count += 1

                        if debug_llm_empty_printed < DEBUG_PRINT_LIMIT:
                            print(
                                f"[DEBUG][LLM_EMPTY] "
                                f"chunk_id={chunk['chunk_id']} "
                                f"paper_id={chunk['paper_id']} "
                                f"ingredients={ingredient_candidates} "
                                f"sentence={sentence}"
                            )
                            debug_llm_empty_printed += 1

                        continue

                    for raw_claim in raw_claims:
                        validated_claim = _validate_claim_compat(
                            raw_claim=raw_claim,
                            sentence=sentence,
                        )

                        if validated_claim is None:
                            validation_fail_count += 1

                            if debug_validation_fail_printed < DEBUG_PRINT_LIMIT:
                                print(
                                    f"[DEBUG][VALIDATION_FAIL] "
                                    f"chunk_id={chunk['chunk_id']} "
                                    f"paper_id={chunk['paper_id']} "
                                    f"ingredients={ingredient_candidates} "
                                    f"raw_claim={raw_claim} "
                                    f"sentence={sentence}"
                                )
                                debug_validation_fail_printed += 1

                            continue

                        claim_row = extractor.build_claim_row(
                            chunk=chunk,
                            sentence=sentence,
                            validated_claim=validated_claim,
                            extraction_method="llm",
                        )

                        claim_id = insert_claim(conn, claim_row)

                        ingredient_id = get_ingredient_id_by_canonical_name(
                            conn,
                            validated_claim["ingredient"],
                        )
                        if ingredient_id is not None:
                            insert_claim_ingredient_map(conn, claim_id, ingredient_id)

                        taxonomy_maps = extractor.infer_taxonomy_maps(
                            validated_claim=validated_claim,
                            effect_rows=effect_rows,
                            concern_rows=concern_rows,
                        )

                        for effect_id in taxonomy_maps["effect_ids"]:
                            insert_claim_effect_map(conn, claim_id, effect_id)

                        for concern_id in taxonomy_maps["concern_ids"]:
                            insert_claim_concern_map(conn, claim_id, concern_id)

                        inserted_claims += 1

                        if debug_inserted_printed < DEBUG_PRINT_LIMIT:
                            print(
                                f"[DEBUG][INSERTED] "
                                f"chunk_id={chunk['chunk_id']} "
                                f"paper_id={chunk['paper_id']} "
                                f"claim={validated_claim}"
                            )
                            debug_inserted_printed += 1

                conn.commit()
                processed_chunks += 1

                if processed_chunks % 20 == 0:
                    print(
                        "[INFO] Processed "
                        f"{processed_chunks} chunks | "
                        f"inserted_claims={inserted_claims} | "
                        f"total_sentences={total_sentences} | "
                        f"blocked={blocked_sentence_count} | "
                        f"non_claim={non_claim_sentence_count} | "
                        f"no_ingredient_candidate={no_ingredient_candidate_count} | "
                        f"llm_empty={llm_empty_count} | "
                        f"validation_fail={validation_fail_count}"
                    )

            except Exception as exc:
                conn.rollback()
                print(f"[ERROR] Failed chunk_id={chunk['chunk_id']}: {exc}")

        print(
            "[INFO] Completed claim extraction | "
            f"processed_chunks={processed_chunks} | "
            f"inserted_claims={inserted_claims} | "
            f"total_sentences={total_sentences} | "
            f"blocked={blocked_sentence_count} | "
            f"non_claim={non_claim_sentence_count} | "
            f"no_ingredient_candidate={no_ingredient_candidate_count} | "
            f"llm_empty={llm_empty_count} | "
            f"validation_fail={validation_fail_count}"
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()