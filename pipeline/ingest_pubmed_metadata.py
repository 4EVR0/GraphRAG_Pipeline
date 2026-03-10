from typing import Dict

from pipeline.config.settings import settings
from pipeline.loaders.ingredient_loader import load_target_ingredients
from pipeline.repositories.paper_repository import (
    get_connection,
    upsert_many_paper_metadata,
)
from pipeline.services.pubmed_client import PubMedClient
from pipeline.services.pubmed_parser import parse_pubmed_xml
from pipeline.services.query_builder import build_pubmed_query


def validate_environment() -> None:
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is not set. Check your .env file.")

    if not settings.ncbi_email:
        raise RuntimeError("NCBI_EMAIL is not set. Please add it to .env.")


def ingest_one_target(client: PubMedClient, conn, target: Dict[str, str]) -> None:
    canonical_name = target["canonical_name"]
    query_name = target["query_name"]
    alias_list = target.get("alias_list", "")
    concern_keywords = target.get("concern_keywords", "")

    query = build_pubmed_query(
        query_name=query_name,
        alias_list=alias_list,
        concern_keywords=concern_keywords,
    )

    print(f"\n[INFO] Target: {canonical_name}")
    print(f"[INFO] Query: {query}")

    pmids = client.search_pmids(query=query, retmax=settings.search_limit)
    print(f"[INFO] Found {len(pmids)} PMIDs")

    if not pmids:
        return

    xml_text = client.fetch_pubmed_xml(pmids)
    if not xml_text:
        print("[WARN] No XML returned from PubMed")
        return

    records = parse_pubmed_xml(xml_text)
    upserted_count = upsert_many_paper_metadata(conn, records)
    conn.commit()

    print(f"[INFO] Upserted {upserted_count} records into paper_metadata")


def main() -> None:
    validate_environment()

    targets = load_target_ingredients(settings.target_csv_path)
    if not targets:
        print("[WARN] No target ingredients found.")
        return

    client = PubMedClient()
    conn = get_connection(settings.database_url)

    try:
        for target in targets:
            try:
                ingest_one_target(client, conn, target)
            except Exception as exc:
                conn.rollback()
                print(f"[ERROR] Failed target={target.get('canonical_name')}: {exc}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()