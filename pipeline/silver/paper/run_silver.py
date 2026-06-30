import argparse
from datetime import datetime, timezone
from typing import Any, Iterable

from oliveyoung_common.batch import build_run_id
from pipeline.chunk.services.chunker import chunker
from pipeline.common.config.settings import settings
from pipeline.common.io.silver_writer import (
    build_silver_metadata,
    ensure_dir,
    write_csv,
    write_json,
)
from pipeline.common.models.silver_record import SilverChunkRecord, SilverPaperRecord
from pipeline.common.repositories.paper_repository import get_connection


SELECT_PAPERS_SQL = """
SELECT
    pmid,
    title,
    abstract_text,
    journal,
    publication_year,
    source_url
FROM paper_metadata
WHERE abstract_text IS NOT NULL
  AND BTRIM(abstract_text) <> ''
  AND pmid IS NOT NULL
ORDER BY pmid
"""


def build_silver_rows(
    papers: Iterable[dict[str, Any]],
    batch_id: str,
) -> tuple[list[dict], list[dict]]:
    paper_rows: list[dict] = []
    chunk_rows: list[dict] = []

    for paper in papers:
        abstract = str(paper["abstract_text"]).strip()
        paper_rows.append(
            SilverPaperRecord(
                batch_id=batch_id,
                pmid=str(paper["pmid"]),
                title=paper.get("title"),
                abstract_text=abstract,
                journal=paper.get("journal"),
                publication_year=paper.get("publication_year"),
                source_url=paper.get("source_url"),
                searched_ingredient_count=0,
                searched_ingredients=None,
            ).to_dict()
        )

        search_from = 0
        for chunk_index, sentence in enumerate(chunker.chunk_abstract_text(abstract)):
            start = abstract.find(sentence, search_from)
            if start < 0:
                start = search_from
            end = start + len(sentence)
            search_from = end
            chunk_rows.append(
                SilverChunkRecord(
                    batch_id=batch_id,
                    pmid=str(paper["pmid"]),
                    chunk_index=chunk_index,
                    section_type="abstract",
                    chunk_text=sentence,
                    char_count=len(sentence),
                    token_count_approx=len(sentence.split()),
                    source_start_offset=start,
                    source_end_offset=end,
                    chunk_version=settings.chunk_version,
                    title=paper.get("title"),
                    journal=paper.get("journal"),
                    publication_year=paper.get("publication_year"),
                    source_url=paper.get("source_url"),
                ).to_dict()
            )

    return paper_rows, chunk_rows


def fetch_papers() -> list[dict[str, Any]]:
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is not set.")

    conn = get_connection(settings.database_url)
    try:
        with conn.cursor() as cur:
            cur.execute(SELECT_PAPERS_SQL)
            columns = [column.name for column in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def main(batch_id: str | None = None) -> str:
    batch_id = batch_id or build_run_id("graphrag_silver_paper")
    papers = fetch_papers()
    paper_rows, chunk_rows = build_silver_rows(papers, batch_id)

    batch_dir = settings.silver_paper_dir / f"batch={batch_id}"
    ensure_dir(batch_dir)
    write_csv(batch_dir / "paper.csv", paper_rows)
    write_csv(batch_dir / "paper_chunk.csv", chunk_rows)
    write_json(
        batch_dir / "metadata.json",
        build_silver_metadata(
            batch_id=batch_id,
            bronze_batch_id="postgres_paper_metadata",
            raw_paper_count=len(papers),
            deduped_paper_count=len(paper_rows),
            chunk_count=len(chunk_rows),
            created_at=datetime.now(timezone.utc).isoformat(),
            chunk_version=settings.chunk_version,
        ),
    )

    print(
        f"[INFO] Silver batch saved to {batch_dir}: "
        f"{len(paper_rows)} papers, {len(chunk_rows)} chunks"
    )
    return batch_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rebuild the complete Silver corpus from paper_metadata."
    )
    parser.add_argument("--batch-id", default=None)
    args = parser.parse_args()
    main(batch_id=args.batch_id)
