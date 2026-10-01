import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from pipeline.gold.claim.evidence_gate import evaluate
from scripts.build_evidence_review_packets import FIELDS, VERSION, build, make_packet
from scripts.fetch_pubmed_review_sources import VERSION as SNAPSHOT_VERSION, parse_articles

XML = b'''<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>123</PMID><Article>
<ArticleTitle>Synthetic study, not clinical evidence</ArticleTitle><Abstract>
<AbstractText Label="METHODS">Adults applied 1% test ingredient gel to forearms for 7 days versus matched vehicle.</AbstractText>
<AbstractText Label="RESULTS">Skin water content significantly increased versus vehicle. TEWL showed no detected difference.</AbstractText>
</Abstract></Article></MedlineCitation></PubmedArticle></PubmedArticleSet>'''
HASH = hashlib.sha256(XML).hexdigest()
PAPER = parse_articles(XML, ["123"])[0]


def draft():
    # Synthetic contract fixture only; does not stand for real independent review.
    values = dict(zip(FIELDS, [None] * len(FIELDS)))
    values.update(ingredient_name="Test ingredient", subject="human_skin", route="topical",
                  claim_kind="efficacy", attribution="isolated_ingredient", study_control="matched_vehicle",
                  result_comparator="matched_vehicle", measured_endpoint="skin_water_content",
                  change_direction="increase", result_support="supported", significance="significant",
                  population="Adults", body_site="forearms", concentration="1%", formulation="gel", duration="7 days")
    result_fields = {"measured_endpoint", "change_direction", "result_support", "significance", "result_comparator"}
    fields = {key: {"value": value, "spans": [{"section": int(key in result_fields),
              "quote": PAPER["abstract_sections"][int(key in result_fields)]["text"]}]} for key, value in values.items()}
    return {"schema_version": VERSION, "source_response_sha256": HASH, "pmid": "123",
            "prepared_by": "synthetic drafter", "prepared_on": "2026-01-01", "fields": fields,
            "result_span": {"section": 1, "quote": PAPER["abstract_sections"][1]["text"]},
            "limitations": ["Synthetic test only"], "note": "Not evidence"}


class AtomicOutcomeReviewTest(unittest.TestCase):
    def test_complete_draft_is_never_approved(self):
        packet = make_packet(draft(), PAPER, HASH)
        self.assertEqual([], packet["pre_review_blockers"])
        self.assertEqual("HYDRATING", packet["proposed_effect_code"])
        self.assertEqual("pending_independent_review", packet["status"])
        self.assertFalse(packet["recommendation_eligible"])
        self.assertNotIn("graph_score", packet)

    def test_study_control_does_not_supply_missing_result_comparison(self):
        for comparator in [None, "baseline", "between_populations"]:
            row = draft()
            if comparator:
                row["fields"]["result_comparator"]["value"] = comparator
            else:
                row["fields"]["result_comparator"] = None
            self.assertIn("outcome_control_required", make_packet(row, PAPER, HASH)["pre_review_blockers"])

    def test_null_outcome_preserved_separately_from_positive_same_paper(self):
        positive = make_packet(draft(), PAPER, HASH)
        row = draft()
        for key, value in {"measured_endpoint": "transepidermal_water_loss", "change_direction": "no_detected_difference",
                           "result_support": "no_detected_effect", "significance": "not_reported"}.items():
            row["fields"][key]["value"] = value
        negative = make_packet(row, PAPER, HASH)
        self.assertNotEqual(positive["observation_id"], negative["observation_id"])
        self.assertIsNone(negative["proposed_effect_code"])
        self.assertIn("positive_result_required", negative["pre_review_blockers"])

    def test_tewl_mapping_does_not_become_barrier_repair(self):
        row = draft()
        paper = copy.deepcopy(PAPER)
        paper["abstract_sections"][1]["text"] = "TEWL significantly decreased versus vehicle."
        for key in ("measured_endpoint", "change_direction", "result_support", "significance", "result_comparator"):
            row["fields"][key]["spans"] = [{"section": 1, "quote": paper["abstract_sections"][1]["text"]}]
        row["result_span"] = {"section": 1, "quote": paper["abstract_sections"][1]["text"]}
        row["fields"]["measured_endpoint"]["value"] = "transepidermal_water_loss"
        row["fields"]["change_direction"]["value"] = "decrease"
        self.assertEqual("MOISTURE_RETENTION", make_packet(row, paper, HASH)["proposed_effect_code"])

    def test_missing_significance_is_not_supplied_by_positive_direction(self):
        row = draft()
        row["fields"]["significance"] = None
        self.assertIn("endpoint_significance_required", make_packet(row, PAPER, HASH)["pre_review_blockers"])

    def test_packet_is_not_a_legacy_gate_approval(self):
        packet = make_packet(draft(), PAPER, HASH)
        source = {"pmid": "123", "ingredient_name": "Test ingredient", "title": PAPER["title"],
                  "source_sentence": PAPER["abstract_sections"][1]["text"]}
        self.assertEqual("review_required", evaluate(source, packet).status)

    def test_missing_context_cannot_be_inferred_from_mesh_or_other_fields(self):
        row = draft()
        row["fields"]["body_site"] = None
        self.assertIn("missing:body_site", make_packet(row, PAPER, HASH)["pre_review_blockers"])

    def test_exact_source_version_and_spans_required(self):
        changes = [lambda d: d.update(source_response_sha256="wrong"),
                   lambda d: d.update(pmid="456"),
                   lambda d: d["result_span"].update(quote="Invented result"),
                   lambda d: d["result_span"].update(section=True),
                   lambda d: d["fields"]["body_site"].update(spans=[]),
                   lambda d: d["fields"]["body_site"].update(value="unknown"),
                   lambda d: d.update(decision="approve"),
                   lambda d: d["fields"].pop("duration")]
        for change in changes:
            row = draft()
            change(row)
            with self.subTest(row=row), self.assertRaises(ValueError):
                make_packet(row, PAPER, HASH)

    def test_models_safety_and_combination_cannot_qualify(self):
        for key, value, blocker in [("subject", "skin_cells", "human_skin_required"),
                                     ("claim_kind", "safety", "efficacy_required"),
                                     ("attribution", "combination", "ingredient_contribution_unresolved")]:
            row = draft()
            row["fields"][key]["value"] = value
            packet = make_packet(row, PAPER, HASH)
            self.assertIn(blocker, packet["pre_review_blockers"])
            self.assertFalse(packet["recommendation_eligible"])

    def test_correction_links_are_not_silently_ignored(self):
        paper = copy.deepcopy(PAPER)
        paper["corrections"] = [{"type": "RetractionIn", "pmid": "456"}]
        self.assertIn("linked_correction_or_retraction_requires_review", make_packet(draft(), paper, HASH)["pre_review_blockers"])

    def test_note_changes_do_not_create_new_observation_but_change_revision_hash(self):
        first = make_packet(draft(), PAPER, HASH)
        row = draft()
        row["note"] = "Updated explanation"
        second = make_packet(row, PAPER, HASH)
        self.assertEqual(first["observation_id"], second["observation_id"])
        self.assertNotEqual(first["draft_sha256"], second["draft_sha256"])

    def setup_files(self, root):
        snapshot = root / "snapshot"
        snapshot.mkdir()
        (snapshot / "pubmed.xml").write_bytes(XML)
        (snapshot / "manifest.json").write_text(json.dumps({"snapshot_version": SNAPSHOT_VERSION,
            "response_sha256": HASH, "requested_pmids": ["123"], "article_count": 1}))
        # Derived content is deliberately invalid: the raw XML is authoritative.
        (snapshot / "papers.jsonl").write_text("tampered derived content")
        annotations = root / "drafts.jsonl"
        annotations.write_text(json.dumps(draft()))
        return snapshot, annotations

    def test_build_preserves_sources_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot, annotations = self.setup_files(root)
            result = build(snapshot, annotations, root / "out")
            self.assertEqual(1, result["observation_count"])
            self.assertEqual(0, result["recommendation_eligible_count"])
            self.assertEqual(XML, (snapshot / "pubmed.xml").read_bytes())
            with self.assertRaises(FileExistsError):
                build(snapshot, annotations, root / "out")

    def test_bad_source_and_duplicate_fail_before_output(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot, annotations = self.setup_files(root)
            annotations.write_text(json.dumps(draft()) + "\n" + json.dumps(draft()))
            with self.assertRaises(ValueError):
                build(snapshot, annotations, root / "out")
            self.assertFalse((root / "out").exists())
            (snapshot / "pubmed.xml").write_bytes(XML + b" ")
            with self.assertRaises(ValueError):
                build(snapshot, annotations, root / "out")
            self.assertFalse((root / "out").exists())


if __name__ == "__main__":
    unittest.main()
