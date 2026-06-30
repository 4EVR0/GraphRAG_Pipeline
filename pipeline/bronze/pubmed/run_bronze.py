import argparse
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from oliveyoung_common.batch import build_run_id
from pipeline.common.config.settings import settings
from pipeline.common.io.bronze_writer import (
    build_batch_metadata,
    ensure_dir,
    write_csv,
    write_json,
)
from pipeline.common.loaders.ingredient_loader import load_target_ingredients
from pipeline.metadata.services.pubmed_client import PubMedClient
from pipeline.metadata.services.pubmed_parser import parse_pubmed_xml
from pipeline.metadata.services.query_builder import build_pubmed_query


def collect_focused_papers(
    targets: list[dict[str, str]],
    search_limit: int,
) -> tuple[list[dict], list[dict]]:
    client = PubMedClient()
    target_by_pmid: dict[str, set[str]] = defaultdict(set)
    queries_by_pmid: dict[str, set[str]] = defaultdict(set)
    search_rows: list[dict] = []

    for target in targets:
        query = build_pubmed_query(
            query_name=target["query_name"],
            alias_list=target.get("alias_list"),
            concern_keywords=target.get("concern_keywords"),
            required_context_keywords=target.get("required_context_keywords"),
            excluded_context_keywords=target.get("excluded_context_keywords"),
        )
        pmids = client.search_pmids(query, search_limit)
        search_rows.append(
            {
                "canonical_name": target["canonical_name"],
                "query": query,
                "pmid_count": len(pmids),
            }
        )
        for pmid in pmids:
            target_by_pmid[pmid].add(target["canonical_name"])
            queries_by_pmid[pmid].add(query)

    records_by_pmid = {}
    all_pmids = sorted(target_by_pmid)
    for start in range(0, len(all_pmids), 200):
        xml_text = client.fetch_pubmed_xml(all_pmids[start : start + 200])
        if not xml_text:
            continue
        for record in parse_pubmed_xml(xml_text):
            if record.pmid:
                records_by_pmid[record.pmid] = record

    paper_rows: list[dict] = []
    for pmid in all_pmids:
        record = records_by_pmid.get(pmid)
        if not record or not record.abstract_text:
            continue
        paper_rows.append(
            {
                **record.to_dict(),
                "searched_ingredients": "|".join(sorted(target_by_pmid[pmid])),
                "search_queries": " || ".join(sorted(queries_by_pmid[pmid])),
            }
        )
    return paper_rows, search_rows


def main(
    target_csv: Path,
    batch_id: str | None = None,
    search_limit: int = 200,
) -> str:
    batch_id = batch_id or build_run_id("graphrag_bronze_pubmed")
    targets = load_target_ingredients(target_csv)
    papers, searches = collect_focused_papers(targets, search_limit)

    batch_dir = settings.bronze_pubmed_dir / f"batch={batch_id}"
    ensure_dir(batch_dir)
    write_csv(batch_dir / "paper_raw.csv", papers)
    write_csv(batch_dir / "search_log.csv", searches)
    write_json(
        batch_dir / "metadata.json",
        build_batch_metadata(
            batch_id=batch_id,
            target_count=len(targets),
            total_search_logs=len(searches),
            total_papers=len(papers),
            created_at=datetime.now(timezone.utc).isoformat(),
        ),
    )
    print(
        f"[INFO] Bronze batch saved to {batch_dir}: "
        f"{len(targets)} targets, {len(papers)} papers"
    )
    return batch_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect focused PubMed corpus.")
    parser.add_argument("--target-csv", type=Path, required=True)
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--search-limit", type=int, default=200)
    args = parser.parse_args()
    main(args.target_csv, batch_id=args.batch_id, search_limit=args.search_limit)
