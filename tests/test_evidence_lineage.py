import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from pipeline.gold.claim.evidence_gate import record_id
from scripts.audit_evidence_lineage import audit, locate_sentence
from scripts.build_evidence_review_packets import FIELDS, VERSION
from scripts.fetch_pubmed_review_sources import VERSION as SNAPSHOT_VERSION, parse_articles

XML = b'''<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>123</PMID><Article>
<ArticleTitle>Synthetic study</ArticleTitle><Abstract>
<AbstractText Label="RESULTS">Test ingredient increased water content. TEWL did not change.</AbstractText>
<AbstractText Label="CONCLUSION">Do not generalize.</AbstractText>
</Abstract></Article></MedlineCitation></PubmedArticle></PubmedArticleSet>'''
HASH = hashlib.sha256(XML).hexdigest()
PAPER = parse_articles(XML, ["123"])[0]


def claim(**changes):
    return {"pmid": "123", "batch_id": "synthetic", "evidence_id": "row-1",
            "ingredient_name": "Legacy ambiguous name", "title": "Synthetic study",
            "source_sentence": "RESULTS: Test ingredient increased water content.",
            "target": "skin hydration", "relation": "reduces", **changes}


class EvidenceLineageTest(unittest.TestCase):
    def setup_files(self, root, rows=None):
        snapshot = root / "snapshot"
        snapshot.mkdir()
        (snapshot / "pubmed.xml").write_bytes(XML)
        (snapshot / "manifest.json").write_text(json.dumps({"snapshot_version": SNAPSHOT_VERSION,
            "response_sha256": HASH, "requested_pmids": ["123"], "article_count": 1}))
        annotations = root / "annotations.jsonl"
        fields = dict.fromkeys(FIELDS)
        span = {"section": 0, "quote": "Test ingredient increased water content."}
        fields["ingredient_name"] = {"value": "Test ingredient", "spans": [span]}
        fields["measured_endpoint"] = {"value": "skin_water_content", "spans": [span]}
        draft = {"schema_version": VERSION, "source_response_sha256": HASH, "pmid": "123",
                 "prepared_by": "synthetic", "prepared_on": "2026-01-01", "fields": fields,
                 "result_span": span, "limitations": ["Synthetic fixture"], "note": "Not evidence"}
        annotations.write_text(json.dumps(draft))
        claims = root / "claims.csv"
        rows = [claim()] if rows is None else rows
        with claims.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        return claims, snapshot, annotations

    def test_same_pmid_and_quote_never_imply_equivalence_or_approval(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            claims, snapshot, annotations = self.setup_files(root)
            before = claims.read_bytes()
            result = audit(claims, snapshot, annotations, root / "out")
            saved = json.loads((root / "out" / "lineage_review.jsonl").read_text())
            row = saved["legacy_claims"][0]
            self.assertEqual(record_id(claim()), row["record_id"])
            self.assertEqual(claim(), row["source"])
            self.assertTrue(row["literal_source_locations"][0]["section_label_removed"])
            self.assertEqual(1, len(row["literal_result_overlap_observation_ids"]))
            self.assertEqual("review_required", row["gate_without_review"]["status"])
            self.assertEqual(0, result["verified_claim_links"])
            self.assertFalse(saved["production_graph_lineage_verified"])
            self.assertFalse(saved["recommendation_eligible"])
            self.assertEqual(before, claims.read_bytes())
            with self.assertRaises(FileExistsError):
                audit(claims, snapshot, annotations, root / "out")

    def test_conclusion_is_not_reported_as_result_overlap(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            args = self.setup_files(root, [claim(source_sentence="CONCLUSION: Do not generalize.")])
            audit(*args, root / "out")
            saved = json.loads((root / "out" / "lineage_review.jsonl").read_text())
            self.assertEqual([], saved["legacy_claims"][0]["literal_result_overlap_observation_ids"])

    def test_missing_source_and_unknown_prefix_not_fuzzy_matched(self):
        self.assertEqual([], locate_sentence("invented text", PAPER))
        self.assertEqual([], locate_sentence("BACKGROUND: Test ingredient increased water content.", PAPER))
        self.assertEqual([], locate_sentence("", PAPER))
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            args = self.setup_files(root, [claim(source_sentence="invented text")])
            result = audit(*args, root / "out")
            self.assertEqual(1, result["counts"]["legacy_rows_requiring_source_location_review"])
            self.assertEqual(0, result["verified_claim_links"])

    def test_absent_means_only_selected_batch_not_global_corpus(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            args = self.setup_files(root, [claim(pmid="456")])
            result = audit(*args, root / "out")
            self.assertEqual(1, result["counts"]["absent_from_selected_batch"])
            self.assertEqual(0, result["matched_unique_legacy_rows"])

    def test_duplicates_keep_all_record_numbers_without_double_count(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            args = self.setup_files(root, [claim(), claim()])
            result = audit(*args, root / "out")
            self.assertEqual(1, result["duplicate_records"])
            self.assertEqual(1, result["matched_unique_legacy_rows"])
            saved = json.loads((root / "out" / "lineage_review.jsonl").read_text())
            self.assertEqual([1, 2], saved["legacy_claims"][0]["csv_record_numbers"])

    def test_mixed_and_missing_batches_fail_before_output(self):
        for rows in [[claim(), claim(batch_id="other")], [claim(batch_id="")],
                     [claim(gold_batch_id="other")]]:
            with self.subTest(rows=rows), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                args = self.setup_files(root, rows)
                with self.assertRaises(ValueError):
                    audit(*args, root / "out")
                self.assertFalse((root / "out").exists())

    def test_invalid_or_duplicate_draft_is_revalidated_before_output(self):
        for mode in ["duplicate", "stale", "bad_quote"]:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                claims, snapshot, annotations = self.setup_files(root)
                draft = json.loads(annotations.read_text())
                if mode == "duplicate":
                    annotations.write_text(json.dumps(draft) + "\n" + json.dumps(draft))
                else:
                    if mode == "stale":
                        draft["source_response_sha256"] = "stale"
                    else:
                        draft["result_span"]["quote"] = "Invented"
                    annotations.write_text(json.dumps(draft))
                with self.assertRaises(ValueError):
                    audit(claims, snapshot, annotations, root / "out")
                self.assertFalse((root / "out").exists())


if __name__ == "__main__":
    unittest.main()
