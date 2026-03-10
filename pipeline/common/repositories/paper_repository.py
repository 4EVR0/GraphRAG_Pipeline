from typing import Iterable

import psycopg2
from psycopg2.extensions import connection

from pipeline.common.models.paper_record import PaperRecord


UPSERT_PAPER_METADATA_SQL = """
INSERT INTO paper_metadata (
    title,
    doi,
    pmid,
    pmcid,
    journal,
    publication_year,
    authors,
    abstract_text,
    study_type,
    evidence_level,
    source_db,
    source_url,
    language_code
)
VALUES (
    %(title)s,
    %(doi)s,
    %(pmid)s,
    %(pmcid)s,
    %(journal)s,
    %(publication_year)s,
    %(authors)s,
    %(abstract_text)s,
    %(study_type)s,
    %(evidence_level)s,
    %(source_db)s,
    %(source_url)s,
    %(language_code)s
)
ON CONFLICT (pmid) DO UPDATE SET
    title = EXCLUDED.title,
    doi = COALESCE(EXCLUDED.doi, paper_metadata.doi),
    pmcid = COALESCE(EXCLUDED.pmcid, paper_metadata.pmcid),
    journal = COALESCE(EXCLUDED.journal, paper_metadata.journal),
    publication_year = COALESCE(EXCLUDED.publication_year, paper_metadata.publication_year),
    authors = COALESCE(EXCLUDED.authors, paper_metadata.authors),
    abstract_text = COALESCE(EXCLUDED.abstract_text, paper_metadata.abstract_text),
    source_db = EXCLUDED.source_db,
    source_url = EXCLUDED.source_url,
    language_code = EXCLUDED.language_code,
    updated_at = NOW();
"""


def get_connection(database_url: str) -> connection:
    return psycopg2.connect(database_url)


def upsert_many_paper_metadata(conn: connection, records: Iterable[PaperRecord]) -> int:
    count = 0
    with conn.cursor() as cur:
        for record in records:
            cur.execute(UPSERT_PAPER_METADATA_SQL, record.to_dict())
            count += 1
    return count