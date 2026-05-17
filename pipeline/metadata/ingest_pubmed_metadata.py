import logging
from typing import Dict

from oliveyoung_common.batch import build_run_id
from oliveyoung_common.logging import job_unit, setup_logging
from pipeline.common.config.settings import settings
from pipeline.common.loaders.ingredient_loader import load_target_ingredients
from pipeline.common.repositories.paper_repository import (
    get_connection,
    upsert_many_paper_metadata,
)
from pipeline.metadata.services.pubmed_client import PubMedClient
from pipeline.metadata.services.pubmed_parser import parse_pubmed_xml
from pipeline.metadata.services.query_builder import build_pubmed_query

setup_logging("graphrag-pubmed-metadata")

logger = logging.getLogger(__name__)


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

    logger.info("Target: %s", canonical_name)
    logger.info("Query: %s", query)

    pmids = client.search_pmids(query=query, retmax=settings.search_limit)
    logger.info("Found %s PMIDs", len(pmids))

    if not pmids:
        return

    xml_text = client.fetch_pubmed_xml(pmids)
    if not xml_text:
        logger.warning("No XML returned from PubMed")
        return

    records = parse_pubmed_xml(xml_text)
    upserted_count = upsert_many_paper_metadata(conn, records)
    conn.commit()

    logger.info("Upserted %s records into paper_metadata", upserted_count)


def main() -> None:
    validate_environment()

    targets = load_target_ingredients(settings.target_ingredients_path)
    if not targets:
        logger.warning("No target ingredients found.")
        return

    client = PubMedClient()
    conn = get_connection(settings.database_url)

    try:
        for target in targets:
            try:
                ingest_one_target(client, conn, target)
            except Exception as exc:
                conn.rollback()
                logger.error("Failed target=%s: %s", target.get("canonical_name"), exc)
    finally:
        conn.close()


if __name__ == "__main__":
    with job_unit(
        logger,
        job="graphrag_pubmed_metadata",
        run_id=build_run_id("graphrag_pubmed_metadata"),
    ):
        main()
