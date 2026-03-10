from typing import Dict, List

from psycopg2.extensions import connection


SELECT_UNPROCESSED_CHUNKS_SQL = """
SELECT
    pc.chunk_id,
    pc.paper_id,
    pc.section_type,
    pc.chunk_text,
    pc.source_start_offset,
    pc.source_end_offset
FROM paper_chunk pc
WHERE pc.section_type = 'abstract'
  AND NOT EXISTS (
      SELECT 1
      FROM extracted_claim ec
      WHERE ec.chunk_id = pc.chunk_id
  )
ORDER BY pc.chunk_id
"""


SELECT_EFFECT_TAXONOMY_SQL = """
SELECT
    effect_id,
    effect_code,
    effect_name_en
FROM effect_taxonomy
WHERE is_active = TRUE
ORDER BY effect_id
"""


SELECT_CONCERN_TAXONOMY_SQL = """
SELECT
    concern_id,
    concern_code,
    concern_name_en
FROM concern_taxonomy
WHERE is_active = TRUE
ORDER BY concern_id
"""


SELECT_INGREDIENT_ID_BY_NAME_SQL = """
SELECT ingredient_id
FROM ingredient_master
WHERE canonical_name = %s
LIMIT 1
"""


INSERT_EXTRACTED_CLAIM_SQL = """
INSERT INTO extracted_claim (
    paper_id,
    chunk_id,
    claim_text,
    normalized_summary,
    claim_type,
    evidence_direction,
    confidence_score,
    section_type,
    extraction_method,
    source_sentence,
    source_start_offset,
    source_end_offset
)
VALUES (
    %(paper_id)s,
    %(chunk_id)s,
    %(claim_text)s,
    %(normalized_summary)s,
    %(claim_type)s,
    %(evidence_direction)s,
    %(confidence_score)s,
    %(section_type)s,
    %(extraction_method)s,
    %(source_sentence)s,
    %(source_start_offset)s,
    %(source_end_offset)s
)
RETURNING claim_id
"""


INSERT_CLAIM_INGREDIENT_MAP_SQL = """
INSERT INTO claim_ingredient_map (
    claim_id,
    ingredient_id,
    role_type,
    confidence_score
)
VALUES (%s, %s, %s, %s)
ON CONFLICT (claim_id, ingredient_id, role_type) DO NOTHING
"""


INSERT_CLAIM_EFFECT_MAP_SQL = """
INSERT INTO claim_effect_map (
    claim_id,
    effect_id,
    confidence_score
)
VALUES (%s, %s, %s)
ON CONFLICT (claim_id, effect_id) DO NOTHING
"""


INSERT_CLAIM_CONCERN_MAP_SQL = """
INSERT INTO claim_concern_map (
    claim_id,
    concern_id,
    confidence_score
)
VALUES (%s, %s, %s)
ON CONFLICT (claim_id, concern_id) DO NOTHING
"""


def fetch_unprocessed_chunks(conn: connection) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute(SELECT_UNPROCESSED_CHUNKS_SQL)
        rows = cur.fetchall()

    return [
        {
            "chunk_id": row[0],
            "paper_id": row[1],
            "section_type": row[2],
            "chunk_text": row[3],
            "source_start_offset": row[4],
            "source_end_offset": row[5],
        }
        for row in rows
    ]


def fetch_effect_taxonomy(conn: connection) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute(SELECT_EFFECT_TAXONOMY_SQL)
        rows = cur.fetchall()

    return [
        {
            "effect_id": row[0],
            "effect_code": row[1],
            "effect_name_en": row[2],
        }
        for row in rows
    ]


def fetch_concern_taxonomy(conn: connection) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute(SELECT_CONCERN_TAXONOMY_SQL)
        rows = cur.fetchall()

    return [
        {
            "concern_id": row[0],
            "concern_code": row[1],
            "concern_name_en": row[2],
        }
        for row in rows
    ]


def get_ingredient_id_by_canonical_name(conn: connection, canonical_name: str):
    with conn.cursor() as cur:
        cur.execute(SELECT_INGREDIENT_ID_BY_NAME_SQL, (canonical_name,))
        row = cur.fetchone()
    return row[0] if row else None


def insert_claim(conn: connection, claim_row: Dict) -> int:
    with conn.cursor() as cur:
        cur.execute(INSERT_EXTRACTED_CLAIM_SQL, claim_row)
        claim_id = cur.fetchone()[0]
    return claim_id


def insert_claim_ingredient_map(
    conn: connection,
    claim_id: int,
    ingredient_id: int,
    confidence_score: float = 0.7,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            INSERT_CLAIM_INGREDIENT_MAP_SQL,
            (claim_id, ingredient_id, "primary", confidence_score),
        )


def insert_claim_effect_map(
    conn: connection,
    claim_id: int,
    effect_id: int,
    confidence_score: float = 0.6,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            INSERT_CLAIM_EFFECT_MAP_SQL,
            (claim_id, effect_id, confidence_score),
        )


def insert_claim_concern_map(
    conn: connection,
    claim_id: int,
    concern_id: int,
    confidence_score: float = 0.6,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            INSERT_CLAIM_CONCERN_MAP_SQL,
            (claim_id, concern_id, confidence_score),
        )