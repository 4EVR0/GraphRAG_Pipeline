import argparse
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from oliveyoung_common.batch import build_run_id
from pipeline.bronze.pubmed.collect_narrow import collect_narrow, load_existing_pmids
from pipeline.common.config.settings import settings
from pipeline.common.io.bronze_writer import (
    build_batch_metadata,
    ensure_dir,
    write_csv,
    write_json,
)
from pipeline.common.loaders.ingredient_loader import load_target_ingredients
from pipeline.metadata.services.narrow_query import (
    load_effect_terms,
    load_ingredient_rules,
    load_pairs,
    load_synonyms,
    plan_pairs,
)
from pipeline.metadata.services.pubmed_client import PubMedClient
from pipeline.metadata.services.pubmed_parser import parse_pubmed_xml
from pipeline.metadata.services.query_builder import build_pubmed_query

MODES = ("ingredient", "narrow")


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


def _resolve(path: Path | str) -> Path:
    path = Path(path)
    return path if path.is_absolute() else settings.base_dir / path


def main_narrow(
    target_csv: Path,
    batch_id: str | None = None,
    pairs_csv: Path | None = None,
    effect_terms_csv: Path | None = None,
    ingredient_rules_csv: Path | None = None,
    use_ingredient_rules: bool = True,
    skip_pmids_from: list[Path] | None = None,
    search_only: bool = False,
    full_fetch_max: int | None = None,
    pair_cap: int | None = None,
) -> str:
    """성분 × 효능 조합별 좁은 검색으로 bronze 배치를 만든다."""
    batch_id = batch_id or build_run_id("graphrag_bronze_pubmed_narrow")
    pairs_csv = _resolve(pairs_csv or settings.narrow_pairs_csv)
    effect_terms_csv = _resolve(effect_terms_csv or settings.narrow_effect_terms_csv)
    ingredient_rules_csv = _resolve(ingredient_rules_csv or settings.narrow_ingredient_rules_csv)
    full_fetch_max = full_fetch_max or settings.narrow_full_fetch_max
    pair_cap = pair_cap or settings.narrow_pair_cap

    rules = load_ingredient_rules(ingredient_rules_csv) if use_ingredient_rules else {}
    plans = plan_pairs(
        load_pairs(pairs_csv), load_synonyms(target_csv), load_effect_terms(effect_terms_csv), rules
    )
    existing = load_existing_pmids(skip_pmids_from or [])
    client = PubMedClient()
    result = collect_narrow(client, plans, full_fetch_max, pair_cap, existing, search_only)

    batch_dir = settings.bronze_pubmed_dir / f"batch={batch_id}"
    ensure_dir(batch_dir)
    if not search_only:
        write_csv(batch_dir / "paper_raw.csv", result.paper_rows)
    write_csv(batch_dir / "search_log.csv", result.search_rows)
    write_csv(batch_dir / "pair_pmids.csv", result.pair_rows)
    metadata = build_batch_metadata(
        batch_id=batch_id,
        target_count=len(plans),
        total_search_logs=len(result.search_rows),
        total_papers=len(result.paper_rows),
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    rows = result.search_rows
    metadata.update(
        {
            "mode": "narrow",
            "search_only": search_only,
            "pairs_csv": str(pairs_csv),
            "effect_terms_csv": str(effect_terms_csv),
            "ingredient_rules_csv": str(ingredient_rules_csv) if use_ingredient_rules else None,
            "full_fetch_max": full_fetch_max,
            "pair_cap": pair_cap,
            "searched_pairs": sum(1 for r in rows if r["query"]),
            "skipped_pairs": {
                reason: sum(1 for r in rows if r["skip_reason"] == reason)
                for reason in sorted({r["skip_reason"] for r in rows if r["skip_reason"]})
            },
            "narrowed_pairs": sum(1 for r in rows if r["narrowed"]),
            "capped_pairs": [f"{r['inci_name']}:{r['effect_code']}" for r in rows if r["capped"]],
            "unique_pmids": len(result.pmids),
            "skipped_existing_pmids": result.skipped_existing,
            "fetched_without_abstract": result.fetched_without_abstract,
            "ncbi_requests": client.request_count,
        }
    )
    write_json(batch_dir / "metadata.json", metadata)
    print(
        f"[INFO] Narrow bronze batch saved to {batch_dir}: {metadata['searched_pairs']} pairs searched, "
        f"{len(result.pmids)} unique PMIDs, {result.skipped_existing} already collected, "
        f"{len(result.paper_rows)} papers written, {client.request_count} NCBI requests"
    )
    return batch_id


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
    parser.add_argument("--mode", choices=MODES, default=settings.pubmed_collection_mode,
                        help="ingredient: 성분별 상위 N건(기존), narrow: 성분×효능 좁은 검색 후 전부")
    parser.add_argument("--target-csv", type=Path, default=None,
                        help="ingredient 모드는 필수. narrow 모드는 동의어 사전으로 쓴다")
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--search-limit", type=int, default=200)
    narrow = parser.add_argument_group("narrow mode")
    narrow.add_argument("--pairs-csv", type=Path, default=None)
    narrow.add_argument("--effect-terms-csv", type=Path, default=None)
    narrow.add_argument("--ingredient-rules-csv", type=Path, default=None)
    narrow.add_argument("--no-ingredient-rules", action="store_true",
                        help="몸속 물질 제외·외용 조건 규칙을 끈다(비교 측정용)")
    narrow.add_argument("--skip-pmids-from", type=Path, action="append", default=[],
                        help="이미 수집한 PMID(배치 디렉터리, CSV, JSON, txt). 여러 번 지정 가능")
    narrow.add_argument("--search-only", action="store_true", help="검색 기록만 남기고 논문은 받지 않는다")
    narrow.add_argument("--full-fetch-max", type=int, default=None)
    narrow.add_argument("--pair-cap", type=int, default=None)
    args = parser.parse_args()
    if args.mode == "narrow":
        main_narrow(
            args.target_csv or settings.target_ingredients_path,
            batch_id=args.batch_id,
            pairs_csv=args.pairs_csv,
            effect_terms_csv=args.effect_terms_csv,
            ingredient_rules_csv=args.ingredient_rules_csv,
            use_ingredient_rules=not args.no_ingredient_rules,
            skip_pmids_from=args.skip_pmids_from,
            search_only=args.search_only,
            full_fetch_max=args.full_fetch_max,
            pair_cap=args.pair_cap,
        )
    else:
        if args.target_csv is None:
            parser.error("--target-csv is required in ingredient mode")
        main(args.target_csv, batch_id=args.batch_id, search_limit=args.search_limit)
