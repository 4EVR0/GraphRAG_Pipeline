#!/usr/bin/env python3
"""Load the PMC PoC store into a localhost-only Neo4j instance.

This intentionally has no URI option: it cannot write to the production graph.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from neo4j import GraphDatabase

from pipeline.fulltext.pmc_poc import ALLOWED_LICENSES


POC_URI = "bolt://127.0.0.1:17687"


def _load_one(tx, paper: dict, passages: list[dict]) -> None:
    tx.run("""
        MERGE (p:PmcPocPaper {pmcid_version: $pmcid_version})
        SET p += $properties
        WITH p
        OPTIONAL MATCH (p)-[:HAS_PASSAGE]->(old:PmcPocPassage)
        DETACH DELETE old
    """, pmcid_version=paper["pmcid_version"], properties=paper).consume()
    tx.run("""
        MATCH (p:PmcPocPaper {pmcid_version: $pmcid_version})
        UNWIND $passages AS row
        CREATE (q:PmcPocPassage {
            passage_id: row.passage_id, corpus: row.corpus,
            section: row.section, ordinal: row.ordinal, text: row.text
        })
        CREATE (p)-[:HAS_PASSAGE]->(q)
    """, pmcid_version=paper["pmcid_version"], passages=passages).consume()


def load_local_neo4j(db_path: Path) -> tuple[int, int]:
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        papers = [dict(row) for row in db.execute("SELECT * FROM paper ORDER BY pmid")]
        if not papers:
            raise ValueError("PoC store contains no papers")
        if any(p["license"] not in ALLOWED_LICENSES for p in papers):
            raise ValueError("PoC store contains a non-allowlisted license")
        if any(not p["pmcid_version"] or not p["source_url"] for p in papers):
            raise ValueError("PoC provenance is incomplete")
        rows = {
            p["pmcid_version"]: [dict(row) for row in db.execute(
                "SELECT passage_id, corpus, section, ordinal, text FROM passage WHERE pmcid_version = ? ORDER BY corpus, ordinal",
                (p["pmcid_version"],),
            )]
            for p in papers
        }
    with GraphDatabase.driver(POC_URI, auth=None, connection_timeout=5) as driver:
        driver.verify_connectivity()
        with driver.session(database="neo4j") as session:
            session.run("CREATE CONSTRAINT pmc_poc_version IF NOT EXISTS FOR (p:PmcPocPaper) REQUIRE p.pmcid_version IS UNIQUE").consume()
            session.run("CREATE CONSTRAINT pmc_poc_passage IF NOT EXISTS FOR (q:PmcPocPassage) REQUIRE q.passage_id IS UNIQUE").consume()
            for paper in papers:
                session.execute_write(_load_one, paper, rows[paper["pmcid_version"]])
            count = session.run("""
                MATCH (p:PmcPocPaper)-[:HAS_PASSAGE]->(q:PmcPocPassage)
                RETURN count(DISTINCT p) AS papers, count(q) AS passages
            """).single()
            return count["papers"], count["passages"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("poc_output/pmc_fulltext/pmc_poc.sqlite3"))
    parser.add_argument("--load-local-neo4j", action="store_true", help="Required explicit acknowledgement of localhost write")
    args = parser.parse_args()
    if not args.load_local_neo4j:
        parser.error("Neo4j write is disabled unless --load-local-neo4j is set")
    papers, passages = load_local_neo4j(args.db)
    print(f"Loaded into isolated local Neo4j: papers={papers} passages={passages} uri={POC_URI}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
