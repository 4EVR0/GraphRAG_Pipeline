#!/usr/bin/env python3
"""Fetch three allowlisted CC BY papers into an isolated local PoC store.

Usage: python -m scripts.run_pmc_fulltext_poc
No LLM API or production Neo4j connection is used.
"""

import argparse
import time
from pathlib import Path

from pipeline.fulltext.pmc_poc import PmcPocError, fetch_article, load_sqlite, write_manifest


DEFAULT_PMIDS = ("32724156", "40682377", "39768384")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pmid", action="append", dest="pmids", help="Override default PMID set; repeat for each paper")
    parser.add_argument("--output-dir", type=Path, default=Path("poc_output/pmc_fulltext"))
    args = parser.parse_args()
    pmids = args.pmids or DEFAULT_PMIDS
    if len(pmids) > 5 or len(pmids) != len(set(pmids)):
        parser.error("This PoC accepts at most five distinct PMIDs")
    articles = []
    for pmid in pmids:
        try:
            article = fetch_article(pmid)
        except (PmcPocError, ValueError) as exc:
            print(f"SKIP PMID={pmid}: {exc}")
            continue
        articles.append(article)
        print(f"OK PMID={pmid} version={article.pmcid_version} license={article.license} body_passages={len(article.body_passages)}")
        time.sleep(0.4)
    if not articles:
        print("No license-approved full texts were loaded")
        return 1
    load_sqlite(args.output_dir / "pmc_poc.sqlite3", articles)
    write_manifest(args.output_dir / "manifest.json", articles)
    print(f"Loaded {len(articles)} article(s) into isolated PoC store: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
