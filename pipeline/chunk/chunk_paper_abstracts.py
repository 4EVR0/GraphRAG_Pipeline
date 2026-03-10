from typing import List

from pipeline.common.config.settings import settings
from pipeline.common.repositories.paper_repository import get_connection
from pipeline.common.repositories.chunk_repository import (
    fetch_papers_with_abstract,
    insert_chunks,
)
from pipeline.chunk.services.chunker import chunk_abstract_text


def validate_environment() -> None:
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is not set. Check your .env file.")


def build_chunk_rows(paper_id: int, abstract_text: str) -> List[dict]:
    chunks = chunk_abstract_text(abstract_text)

    chunk_rows = []
    current_offset = 0

    for idx, chunk in enumerate(chunks):
        start_offset = abstract_text.find(chunk, current_offset)
        if start_offset == -1:
            start_offset = current_offset

        end_offset = start_offset + len(chunk)
        current_offset = end_offset

        chunk_rows.append(
            {
                "paper_id": paper_id,
                "section_type": "abstract",
                "chunk_index": idx,
                "chunk_text": chunk,
                "token_count": None,
                "char_count": len(chunk),
                "source_start_offset": start_offset,
                "source_end_offset": end_offset,
            }
        )

    return chunk_rows


def main() -> None:
    validate_environment()

    conn = get_connection(settings.database_url)

    try:
        papers = fetch_papers_with_abstract(conn, only_unchunked=True)
        print(f"[INFO] Found {len(papers)} papers to chunk")

        total_inserted = 0

        for paper_id, abstract_text in papers:
            try:
                chunk_rows = build_chunk_rows(paper_id, abstract_text)
                inserted = insert_chunks(conn, chunk_rows)
                conn.commit()

                total_inserted += inserted
                print(
                    f"[INFO] paper_id={paper_id} | chunks={len(chunk_rows)} | inserted={inserted}"
                )
            except Exception as exc:
                conn.rollback()
                print(f"[ERROR] Failed paper_id={paper_id}: {exc}")

        print(f"[INFO] Total inserted chunks: {total_inserted}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()