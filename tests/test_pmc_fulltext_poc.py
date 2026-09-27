import sqlite3
import tempfile
import unittest
from pathlib import Path

from pipeline.fulltext.pmc_poc import PmcPocError, load_sqlite, parse_article


def sample_bioc(license_name="CC BY", pmcid="PMC123"):
    return [{"documents": [{"id": pmcid, "infons": {"license": license_name}, "passages": [
        {"infons": {"section_type": "ABSTRACT"}, "text": "This abstract reports a measurable reduction in acne lesion count."},
        {"infons": {"section_type": "RESULTS"}, "text": "The study group had a measurable reduction in acne lesion count."},
        {"infons": {"section_type": "REF"}, "text": "A reference that must never be loaded as evidence."},
    ]}]}]


def sample_xml(pmid="12345", pmcid="PMC123", license_url="https://creativecommons.org/licenses/by/4.0/"):
    return f'''<pmc-articleset><article article-type="research-article"><front><article-meta>
      <article-id pub-id-type="pmid">{pmid}</article-id>
      <article-id pub-id-type="pmcid">{pmcid}</article-id>
      <article-id pub-id-type="pmcid-ver">{pmcid}.1</article-id>
      <article-id pub-id-type="doi">10.1234/example</article-id>
      <title-group><article-title>Example trial</article-title></title-group>
      <permissions><license><license-p>{license_url}</license-p></license></permissions>
    </article-meta></front></article></pmc-articleset>'''.encode()


class PmcFulltextPocTest(unittest.TestCase):
    def test_parse_and_load_keeps_version_license_and_source_passages(self):
        article = parse_article(sample_bioc(), sample_xml(), "12345")
        self.assertEqual(article.pmcid_version, "PMC123.1")
        self.assertEqual(article.license, "CC BY")
        self.assertEqual(len(article.abstract_passages), 1)
        self.assertEqual(len(article.body_passages), 1)
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "poc.sqlite3"
            load_sqlite(db_path, [article])
            load_sqlite(db_path, [article])  # Rerun must be idempotent.
            with sqlite3.connect(db_path) as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM paper").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM passage").fetchone()[0], 2)
                self.assertEqual(db.execute("SELECT license FROM paper").fetchone()[0], "CC BY")

    def test_noncommercial_license_is_rejected(self):
        with self.assertRaisesRegex(PmcPocError, "not allowlisted"):
            parse_article(sample_bioc("CC BY-NC"), sample_xml(), "12345")

    def test_mismatched_paper_id_is_rejected(self):
        with self.assertRaisesRegex(PmcPocError, "identifiers disagree"):
            parse_article(sample_bioc(), sample_xml(pmid="99999"), "12345")

    def test_mismatched_license_is_rejected(self):
        with self.assertRaisesRegex(PmcPocError, "licenses disagree"):
            parse_article(sample_bioc(), sample_xml(license_url="https://creativecommons.org/licenses/by-nc/4.0/"), "12345")


if __name__ == "__main__":
    unittest.main()
