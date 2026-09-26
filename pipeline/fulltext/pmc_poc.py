"""Small, license-gated PMC full-text ingestion experiment.

Only official NCBI BioC and E-utilities endpoints are used. The SQLite store is
isolated from the production graph and keeps each passage linked to its exact
article version and license. This module does not extract efficacy claims.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests


BIOC_URL = "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json/{pmid}/unicode"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
ALLOWED_LICENSES = frozenset({"CC0", "CC BY"})
BODY_SECTIONS = frozenset({"INTRO", "METHODS", "RESULTS", "DISCUSS", "CONCL"})


class PmcPocError(ValueError):
    """A paper is unavailable or unsafe for this deliberately narrow PoC."""


@dataclass(frozen=True)
class Passage:
    passage_id: str
    section: str
    ordinal: int
    text: str


@dataclass(frozen=True)
class Article:
    pmid: str
    pmcid: str
    pmcid_version: str
    doi: str | None
    title: str
    source_url: str
    license: str
    license_url: str | None
    retrieved_at: str
    content_sha256: str
    abstract_passages: tuple[Passage, ...]
    body_passages: tuple[Passage, ...]


def _normalise(text: str) -> str:
    return " ".join(text.split())


def parse_article(bioc_json: object, pmc_xml: bytes, requested_pmid: str) -> Article:
    """Validate the ID/license join before accepting any full-text passages."""
    if not requested_pmid.isdecimal():
        raise PmcPocError("PMID must be numeric")
    if not isinstance(bioc_json, list) or not bioc_json:
        raise PmcPocError("BioC full text unavailable")
    documents = bioc_json[0].get("documents", [])
    if len(documents) != 1:
        raise PmcPocError("Expected exactly one BioC document")
    document = documents[0]
    pmcid = document.get("id", "")
    if not re.fullmatch(r"PMC\d+", pmcid):
        raise PmcPocError("BioC PMCID missing")
    license_name = document.get("infons", {}).get("license")
    if license_name not in ALLOWED_LICENSES:
        raise PmcPocError(f"License not allowlisted: {license_name or 'unknown'}")

    root = ET.fromstring(pmc_xml)
    article = root.find("article") if root.tag == "pmc-articleset" else root
    if article is None or article.tag != "article":
        raise PmcPocError("PMC XML article missing")
    if article.attrib.get("article-type") in {"retraction", "correction", "expression-of-concern"}:
        raise PmcPocError("Non-original article status")
    meta = article.find("./front/article-meta")
    if meta is None:
        raise PmcPocError("PMC article metadata missing")
    ids = {x.attrib.get("pub-id-type"): (x.text or "").strip() for x in meta.findall("article-id")}
    if ids.get("pmid") != requested_pmid or ids.get("pmcid") != pmcid:
        raise PmcPocError("BioC and PMC XML identifiers disagree")
    version = ids.get("pmcid-ver", "")
    if not re.fullmatch(re.escape(pmcid) + r"\.\d+", version):
        raise PmcPocError("Versioned PMCID missing")

    license_element = meta.find("./permissions/license")
    license_text = " ".join(license_element.itertext()).lower() if license_element is not None else ""
    license_url_match = re.search(r"https?://creativecommons\.org/(?:publicdomain/zero|licenses/[a-z-]+)/[\d.]+/?", license_text)
    license_url = license_url_match.group(0) if license_url_match else None
    if license_url is None:
        raise PmcPocError("Machine-readable license URL missing from PMC XML")
    expected_path = "/publicdomain/zero/" if license_name == "CC0" else "/licenses/by/"
    if expected_path not in license_url:
        raise PmcPocError("BioC and PMC XML licenses disagree")

    title_element = meta.find("./title-group/article-title")
    title = _normalise(" ".join(title_element.itertext())) if title_element is not None else ""
    if not title:
        raise PmcPocError("Article title missing")
    abstract: list[Passage] = []
    body: list[Passage] = []
    for item in document.get("passages", []):
        section = (item.get("infons") or {}).get("section_type", "")
        if section != "ABSTRACT" and section not in BODY_SECTIONS:
            continue
        text = _normalise(item.get("text") or "")
        if len(text) < 40:
            continue
        target = abstract if section == "ABSTRACT" else body
        ordinal = len(target)
        passage_id = hashlib.sha256(f"{version}:{section}:{ordinal}:{text}".encode()).hexdigest()[:24]
        target.append(Passage(passage_id, section, ordinal, text))
    if not abstract or not body or not any(p.section == "RESULTS" for p in body):
        raise PmcPocError("Abstract or results-bearing body text missing")
    content_hash = hashlib.sha256("\n".join(p.text for p in abstract + body).encode()).hexdigest()
    return Article(
        pmid=requested_pmid,
        pmcid=pmcid,
        pmcid_version=version,
        doi=ids.get("doi") or None,
        title=title,
        source_url=f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/",
        license=license_name,
        license_url=license_url,
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        content_sha256=content_hash,
        abstract_passages=tuple(abstract),
        body_passages=tuple(body),
    )


def fetch_article(pmid: str, session: requests.Session | None = None) -> Article:
    """Fetch one PMID without scraping PMC article pages."""
    if not pmid.isdecimal():
        raise PmcPocError("PMID must be numeric")
    client = session or requests.Session()
    bioc = client.get(BIOC_URL.format(pmid=pmid), timeout=30)
    bioc.raise_for_status()
    if "json" not in bioc.headers.get("content-type", "").lower():
        raise PmcPocError("BioC full text unavailable")
    data = bioc.json()
    documents = data[0].get("documents", []) if isinstance(data, list) and data else []
    pmcid = documents[0].get("id", "") if len(documents) == 1 else ""
    if not re.fullmatch(r"PMC\d+", pmcid):
        raise PmcPocError("BioC PMCID missing")
    time.sleep(0.4)  # Remain below NCBI's unauthenticated request rate.
    response = client.get(EFETCH_URL, params={"db": "pmc", "id": pmcid[3:], "retmode": "xml"}, timeout=30)
    response.raise_for_status()
    return parse_article(data, response.content, pmid)


def load_sqlite(path: Path, articles: list[Article]) -> None:
    """Load a disposable, standalone paper--passage graph; no production DB writes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as db:
        db.execute("PRAGMA foreign_keys = ON")
        db.executescript("""
            CREATE TABLE IF NOT EXISTS paper (
                pmcid_version TEXT PRIMARY KEY, pmid TEXT NOT NULL,
                pmcid TEXT NOT NULL, doi TEXT, title TEXT NOT NULL,
                source_url TEXT NOT NULL,
                license TEXT NOT NULL, license_url TEXT NOT NULL,
                retrieved_at TEXT NOT NULL, content_sha256 TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS passage (
                passage_id TEXT PRIMARY KEY, pmcid_version TEXT NOT NULL,
                corpus TEXT NOT NULL CHECK (corpus IN ('abstract', 'fulltext')),
                section TEXT NOT NULL, ordinal INTEGER NOT NULL, text TEXT NOT NULL,
                FOREIGN KEY(pmcid_version) REFERENCES paper(pmcid_version)
            );
        """)
        existing_columns = {row[1] for row in db.execute("PRAGMA table_info(paper)")}
        if "source_url" not in existing_columns:
            db.execute("ALTER TABLE paper ADD COLUMN source_url TEXT NOT NULL DEFAULT ''")
        for article in articles:
            db.execute("""INSERT INTO paper (
                    pmcid_version, pmid, pmcid, doi, title, source_url,
                    license, license_url, retrieved_at, content_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pmcid_version) DO UPDATE SET
                    pmid=excluded.pmid, pmcid=excluded.pmcid, doi=excluded.doi,
                    title=excluded.title, source_url=excluded.source_url,
                    license=excluded.license,
                    license_url=excluded.license_url, retrieved_at=excluded.retrieved_at,
                    content_sha256=excluded.content_sha256""", (
                article.pmcid_version, article.pmid, article.pmcid, article.doi,
                article.title, article.source_url, article.license, article.license_url,
                article.retrieved_at, article.content_sha256,
            ))
            db.execute("DELETE FROM passage WHERE pmcid_version = ?", (article.pmcid_version,))
            for corpus, passages in (("abstract", article.abstract_passages), ("fulltext", article.body_passages)):
                db.executemany("""INSERT INTO passage VALUES (?, ?, ?, ?, ?, ?)""", [
                    (p.passage_id, article.pmcid_version, corpus, p.section, p.ordinal, p.text)
                    for p in passages
                ])


def write_manifest(path: Path, articles: list[Article]) -> None:
    """Write metadata only; the full text stays in the ignored SQLite file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "pmid": a.pmid, "pmcid": a.pmcid, "pmcid_version": a.pmcid_version,
            "doi": a.doi, "title": a.title, "source_url": a.source_url,
            "license": a.license,
            "license_url": a.license_url, "retrieved_at": a.retrieved_at,
            "content_sha256": a.content_sha256,
            "abstract_passage_count": len(a.abstract_passages),
            "body_passage_count": len(a.body_passages),
        }
        for a in articles
    ]
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
