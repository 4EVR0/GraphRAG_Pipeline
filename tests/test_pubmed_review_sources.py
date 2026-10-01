import json
import tempfile
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

from scripts.fetch_pubmed_review_sources import capture, parse_articles, validate_pmids

XML = b'''<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>123</PMID><Article>
<ArticleTitle>Synthetic <i>skin</i> study</ArticleTitle><Abstract>
<AbstractText Label="METHODS" NlmCategory="METHODS">Human skin, topical.</AbstractText>
<AbstractText Label="RESULTS" NlmCategory="RESULTS">Hydration increased; TEWL unchanged.</AbstractText>
</Abstract><PublicationTypeList><PublicationType>Clinical Trial</PublicationType></PublicationTypeList>
</Article><MeshHeadingList><MeshHeading><DescriptorName>Humans</DescriptorName></MeshHeading></MeshHeadingList>
<CommentsCorrectionsList><CommentsCorrections RefType="ErratumIn"><PMID>456</PMID></CommentsCorrections></CommentsCorrectionsList>
</MedlineCitation><PubmedData><ArticleIdList><ArticleId IdType="doi">synthetic</ArticleId></ArticleIdList></PubmedData>
</PubmedArticle></PubmedArticleSet>'''


class PubmedReviewSnapshotTest(unittest.TestCase):
    def test_parse_preserves_context_and_corrections_without_approval(self):
        paper = parse_articles(XML, ["123"])[0]
        self.assertEqual("Synthetic skin study", paper["title"])
        self.assertEqual(2, len(paper["abstract_sections"]))
        self.assertEqual("METHODS", paper["abstract_sections"][0]["label"])
        self.assertEqual("456", paper["corrections"][0]["pmid"])
        self.assertEqual(["Humans"], paper["mesh_terms"])
        self.assertEqual("source_snapshot_not_reviewed_evidence", paper["status"])

    def test_mismatched_duplicate_and_error_response_fail_closed(self):
        with self.assertRaises(ValueError):
            parse_articles(XML, ["124"])
        with self.assertRaises(ValueError):
            parse_articles(b"<eFetchResult><ERROR>Rate limit</ERROR></eFetchResult>", ["123"])
        with self.assertRaises(ValueError):
            parse_articles(XML.replace(b"</PubmedArticleSet>", XML.split(b"<PubmedArticleSet>")[1]), ["123"])

    def test_missing_abstract_is_explicit(self):
        raw = b"<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>123</PMID><Article><ArticleTitle>Title</ArticleTitle></Article></MedlineCitation></PubmedArticle></PubmedArticleSet>"
        self.assertEqual("missing", parse_articles(raw, ["123"])[0]["abstract_status"])

    def test_invalid_requests_do_not_fetch(self):
        for pmids in [[], ["1", "1"], ["0"], ["1&api_key=bad"], [str(i) for i in range(1, 12)]]:
            with self.subTest(pmids=pmids), self.assertRaises(ValueError):
                validate_pmids(pmids)

    def test_capture_records_exact_response_hash_and_cannot_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "snapshot"
            with patch("urllib.request.urlopen", return_value=BytesIO(XML)) as fetch:
                result = capture(["123"], out)
            self.assertEqual(XML, (out / "pubmed.xml").read_bytes())
            paper = json.loads((out / "papers.jsonl").read_text())
            self.assertEqual(result["response_sha256"], paper["source_response_sha256"])
            fetch.assert_called_once()
            with patch("urllib.request.urlopen") as fetch, self.assertRaises(FileExistsError):
                capture(["123"], out)
            fetch.assert_not_called()
