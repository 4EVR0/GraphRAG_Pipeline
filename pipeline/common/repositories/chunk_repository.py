from typing import List, Tuple

from psycopg2.extensions import connection


SELECT_PAPERS_WITH_ABSTRACT_SQL = """
SELECT
    paper_id,
    abstract_text
FROM paper_metadata
WHERE abstract_text IS NOT NULL
  AND TRIM(abstract_text) <> ''
ORDER BY paper_id
"""


SELECT_UNCHUNKED_PAPERS_WITH_ABSTRACT_SQL = """
SELECT
    pm.paper_id,
    pm.abstract_text
FROM paper_metadata pm
WHERE pm.abstract_text IS NOT NULL
  AND TRIM(pm.abstract_text) <> ''
  AND NOT EXISTS (
      SELECT 1
      FROM paper_chunk pc
      WHERE pc.paper_id = pm.paper_id
        AND pc.section_type = 'abstract'
  )
ORDER BY pm.paper_id
"""


INSERT_PAPER_CHUNK_SQL = """
INSERT INTO paper_chunk (
    paper_id,
    section_type,
    chunk_index,
    chunk_text,
    token_count,
    char_count,
    source_start_offset,
    source_end_offset
)
VALUES (
    %(paper_id)s,
    %(section_type)s,
    %(chunk_index)s,
    %(chunk_text)s,
    %(token_count)s,
    %(char_count)s,
    %(source_start_offset)s,
    %(source_end_offset)s
)
ON CONFLICT (paper_id, section_type, chunk_index) DO NOTHING
"""


def fetch_papers_with_abstract(conn: connection, only_unchunked: bool = True) -> List[Tuple[int, str]]:
    sql = SELECT_UNCHUNKED_PAPERS_WITH_ABSTRACT_SQL if only_unchunked else SELECT_PAPERS_WITH_ABSTRACT_SQL

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return rows


def insert_chunks(conn: connection, chunk_rows: List[dict]) -> int:
    if not chunk_rows:
        return 0

    with conn.cursor() as cur:
        for row in chunk_rows:
            cur.execute(INSERT_PAPER_CHUNK_SQL, row)

    return len(chunk_rows)