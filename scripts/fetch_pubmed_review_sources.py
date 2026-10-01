"""Capture at most ten explicitly selected PubMed abstracts for local review.

No full text, LLM, graph import or automatic evidence approval. Keeps the exact
NCBI response and hashes so study context can be checked outside isolated claims.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

ENDPOINT = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
VERSION = "pubmed-review-snapshot-v1"


def validate_pmids(pmids: list[str]) -> None:
    if not 1 <= len(pmids) <= 10 or len(set(pmids)) != len(pmids):
        raise ValueError("Select 1–10 distinct PMIDs; bulk corpus fetching is not supported")
    if any(not re.fullmatch(r"[1-9][0-9]*", pmid) for pmid in pmids):
        raise ValueError("Each PMID must be a positive integer string")


def content(element: ET.Element | None) -> str:
    return "" if element is None else "".join(element.itertext()).strip()


def parse_articles(raw: bytes, pmids: list[str]) -> list[dict]:
    validate_pmids(pmids)
    root = ET.fromstring(raw)
    found = {}
    for node in root.findall("./PubmedArticle"):
        citation = node.find("MedlineCitation")
        pmid = content(citation.find("PMID")) if citation is not None else ""
        if pmid in found or pmid not in pmids:
            raise ValueError("Unexpected or duplicate PMID in NCBI response")
        article = citation.find("Article")
        if article is None or not content(article.find("ArticleTitle")):
            raise ValueError("Article title missing")
        sections = [{
            "label": section.get("Label", ""),
            "category": section.get("NlmCategory", ""),
            "text": content(section),
        } for section in article.findall("./Abstract/AbstractText")]
        found[pmid] = {
            "pmid": pmid, "source_url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
            "title": content(article.find("ArticleTitle")),
            "abstract_sections": sections,
            "abstract_status": "available" if any(s["text"] for s in sections) else "missing",
            "publication_types": [content(x) for x in article.findall("./PublicationTypeList/PublicationType")],
            "mesh_terms": [content(x) for x in citation.findall("./MeshHeadingList/MeshHeading/DescriptorName")],
            "identifiers": {x.get("IdType", "unknown"): content(x) for x in node.findall("./PubmedData/ArticleIdList/ArticleId")},
            "corrections": [{
                "type": item.get("RefType", ""), "pmid": content(item.find("PMID")),
                "citation": content(item.find("RefSource")),
            } for item in citation.findall("./CommentsCorrectionsList/CommentsCorrections")],
            "status": "source_snapshot_not_reviewed_evidence",
        }
    if set(found) != set(pmids):
        raise ValueError("NCBI response missing one or more requested PMIDs")
    return [found[pmid] for pmid in pmids]


def capture(pmids: list[str], output: Path) -> dict:
    validate_pmids(pmids)
    if output.exists():
        raise FileExistsError("Use a new isolated source snapshot directory")
    query = urllib.parse.urlencode({
        "db": "pubmed", "id": ",".join(pmids), "retmode": "xml",
        "tool": "4evr0_evidence_review",
    })
    url = f"{ENDPOINT}?{query}"
    request = urllib.request.Request(url, headers={"User-Agent": "4EVR0-EvidenceReview/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read(10_000_001)
    if len(raw) > 10_000_000:
        raise ValueError("Unexpectedly large response; no snapshot saved")
    articles = parse_articles(raw, pmids)
    output.mkdir(parents=True)
    raw_hash = hashlib.sha256(raw).hexdigest()
    with (output / "pubmed.xml").open("xb") as handle:
        handle.write(raw)
    with (output / "papers.jsonl").open("x", encoding="utf-8") as handle:
        for article in articles:
            article.update(source_response_sha256=raw_hash, snapshot_version=VERSION)
            handle.write(json.dumps(article, ensure_ascii=False) + "\n")
    manifest = {
        "snapshot_version": VERSION, "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "requested_pmids": pmids, "request_url": url, "response_sha256": raw_hash,
        "article_count": len(articles),
        "missing_abstract_pmids": [a["pmid"] for a in articles if a["abstract_status"] == "missing"],
        "scope": "abstract_metadata_only_no_claim_extraction_or_gate_approval",
    }
    with (output / "manifest.json").open("x", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pmid", required=True, action="append")
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(capture(args.pmid, args.output_dir), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
