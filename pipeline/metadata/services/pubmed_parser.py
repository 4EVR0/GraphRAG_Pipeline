from typing import List, Optional
from xml.etree import ElementTree as ET

from pipeline.common.models.bronze_record import BronzePaperRecord
from pipeline.common.models.paper_record import PaperRecord


def extract_text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None:
        return None

    text = "".join(elem.itertext()).strip()
    return text or None


def _parse_article_core(article: ET.Element) -> dict:
    medline = article.find("./MedlineCitation")
    pubmed_data = article.find("./PubmedData")
    article_node = medline.find("./Article") if medline is not None else None

    pmid = extract_text(medline.find("./PMID")) if medline is not None else None
    title = extract_text(article_node.find("./ArticleTitle")) if article_node is not None else None

    abstract_parts: List[str] = []
    if article_node is not None:
        for ab in article_node.findall("./Abstract/AbstractText"):
            text = extract_text(ab)
            if not text:
                continue
            label = ab.attrib.get("Label")
            abstract_parts.append(f"{label}: {text}" if label else text)

    abstract_text = "\n".join(abstract_parts) if abstract_parts else None
    journal = extract_text(article_node.find("./Journal/Title")) if article_node is not None else None

    publication_year = None
    if article_node is not None:
        year_text = extract_text(article_node.find("./Journal/JournalIssue/PubDate/Year"))
        if year_text and year_text.isdigit():
            publication_year = int(year_text)
        else:
            medline_date = extract_text(
                article_node.find("./Journal/JournalIssue/PubDate/MedlineDate")
            )
            if medline_date and medline_date[:4].isdigit():
                publication_year = int(medline_date[:4])

    authors: List[str] = []
    if article_node is not None:
        for author in article_node.findall("./AuthorList/Author"):
            last_name = extract_text(author.find("./LastName"))
            fore_name = extract_text(author.find("./ForeName"))
            collective_name = extract_text(author.find("./CollectiveName"))

            if collective_name:
                authors.append(collective_name)
            elif last_name and fore_name:
                authors.append(f"{fore_name} {last_name}")
            elif last_name:
                authors.append(last_name)

    doi = None
    if article_node is not None:
        for eid in article_node.findall("./ELocationID"):
            if eid.attrib.get("EIdType") == "doi":
                doi = extract_text(eid)
                break

    pmcid = None
    if pubmed_data is not None:
        for aid in pubmed_data.findall("./ArticleIdList/ArticleId"):
            if aid.attrib.get("IdType") == "pmc":
                pmcid = extract_text(aid)
                break

    source_url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else None

    return {
        "title": title,
        "doi": doi,
        "pmid": pmid,
        "pmcid": pmcid,
        "journal": journal,
        "publication_year": publication_year,
        "authors": "; ".join(authors) if authors else None,
        "abstract_text": abstract_text,
        "source_url": source_url,
        "language_code": "en",
    }


def parse_pubmed_xml(xml_text: str) -> List[PaperRecord]:
    root = ET.fromstring(xml_text)
    records: List[PaperRecord] = []

    for article in root.findall(".//PubmedArticle"):
        core = _parse_article_core(article)
        records.append(
            PaperRecord(
                title=core["title"],
                doi=core["doi"],
                pmid=core["pmid"],
                pmcid=core["pmcid"],
                journal=core["journal"],
                publication_year=core["publication_year"],
                authors=core["authors"],
                abstract_text=core["abstract_text"],
                study_type=None,
                evidence_level=None,
                source_db="PubMed",
                source_url=core["source_url"],
                language_code=core["language_code"],
            )
        )

    return records


def parse_pubmed_xml_to_bronze_records(
    xml_text: str,
    batch_id: str,
    canonical_name: str,
    query_name: str,
    alias_list: str | None,
    concern_keywords: str | None,
    final_query: str,
) -> List[BronzePaperRecord]:
    root = ET.fromstring(xml_text)
    records: List[BronzePaperRecord] = []

    for article in root.findall(".//PubmedArticle"):
        core = _parse_article_core(article)
        records.append(
            BronzePaperRecord(
                batch_id=batch_id,
                source="PubMed",
                searched_ingredient=canonical_name,
                query_name=query_name,
                alias_list=alias_list or None,
                concern_keywords=concern_keywords or None,
                final_query=final_query,
                pmid=core["pmid"],
                pmcid=core["pmcid"],
                doi=core["doi"],
                title=core["title"],
                abstract_text=core["abstract_text"],
                journal=core["journal"],
                publication_year=core["publication_year"],
                authors=core["authors"],
                source_url=core["source_url"],
                language_code=core["language_code"],
            )
        )

    return records