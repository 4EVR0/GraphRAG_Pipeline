import csv
import json
import tempfile
import unittest
from pathlib import Path

from pipeline.gold.claim.evidence_gate import (
    GATE_VERSION, SCHEMA_VERSION, candidate_edge, evaluate, record_id,
)
from scripts.audit_evidence_gate import audit, load_reviews


def source(**changes):
    # Synthetic positive control; not a real article and never a curated record.
    return {
        "pmid": "999999999", "ingredient_name": "Test ingredient",
        "title": "Synthetic controlled human skin study",
        "source_sentence": "Test ingredient significantly increased skin water content.",
        "target": "skin hydration", "relation": "increases", "claim_type": "efficacy",
        "study_context": "human_topical", "gold_batch_id": "synthetic",
        "evidence_id": "synthetic-1", **changes,
    }


def approved(row, **changes):
    return {
        "schema_version": SCHEMA_VERSION, "record_id": record_id(row),
        "decision": "approve", "reviewer": "synthetic-test-reviewer",
        "reviewed_on": "2026-01-01", "source_verified": True,
        "supporting_quote": row["source_sentence"], "source_locator": "abstract/results",
        "review_note": "Synthetic fixture only.", "subject": "human_skin", "route": "topical",
        "claim_kind": "efficacy", "attribution": "isolated_ingredient",
        "comparator": "matched_vehicle", "effect_code": "HYDRATING",
        "measured_endpoint": "skin_water_content", "change_direction": "increase",
        "result_support": "supported", "significance": "significant",
        "claim_entailment_verified": True, "population": "healthy adults",
        "body_site": "forearm", "concentration": "1%", "formulation": "test gel",
        "duration": "7 days", "limitations": ["Not evidence for marketed products"],
        "context_excerpt": "Synthetic methods: 1% gel on adult forearms for 7 days versus vehicle.",
        "context_source_url": f"https://pubmed.ncbi.nlm.nih.gov/{row['pmid']}/", **changes,
    }


class EvidenceGateTest(unittest.TestCase):
    def test_positive_control_preserves_conditions_and_provenance(self):
        row = source()
        review = approved(row)
        decision = evaluate(row, review)
        self.assertEqual("candidate", decision.status)
        edge = candidate_edge(row, review, decision)
        self.assertEqual("1%", edge["constraints"]["concentration"])
        self.assertEqual(row["pmid"], edge["pmid"])
        self.assertEqual(record_id(review), edge["review_sha256"])
        self.assertEqual(GATE_VERSION, edge["gate_version"])
        self.assertNotIn("graph_score", edge)

    def test_legacy_human_topical_label_never_auto_approves(self):
        self.assertEqual("review_required", evaluate(source()).status)

    def test_non_skin_incidents_flagged_despite_human_topical_label(self):
        for title in [
            "Beeswax-poly(vinyl alcohol) composite films for bread packaging.",
            "Functional feed ingredients modulate the immune response of RTgutGC cells.",
            "Cholesterol depletion-induced esophageal dysfunction in GERD.",
        ]:
            with self.subTest(title=title):
                row = source(title=title)
                self.assertIn("suspected_non_skin_context", evaluate(row).reasons)
                self.assertEqual("review_required", evaluate(row, approved(row)).status)
                self.assertEqual("reject", evaluate(row, approved(row, subject="non_skin")).status)

    def test_tewl_decrease_is_beneficial_not_barrier_decrease(self):
        row = source(source_sentence="Test ingredient significantly decreased transepidermal water loss.",
                     target="transepidermal water loss", relation="reduces")
        review = approved(row, effect_code="MOISTURE_RETENTION",
                          measured_endpoint="transepidermal_water_loss", change_direction="decrease")
        self.assertEqual("candidate", evaluate(row, review).status)
        for changes in [{"change_direction": "increase"}, {"effect_code": "BARRIER_REPAIR"}]:
            self.assertEqual("review_required", evaluate(row, {**review, **changes}).status)

    def test_caffeine_legacy_reversal_requires_review(self):
        row = source(ingredient_name="Caffeine", relation="reduces", target="skin barrier function",
                     source_sentence="Caffeine reduced TEWL in male skin compared with female skin.")
        self.assertIn("endpoint_direction_requires_review", evaluate(row).reasons)

    def test_skin_models_kept_without_ranking(self):
        row = source()
        for subject in ["skin_cells", "reconstructed_skin", "animal_skin", "ex_vivo_skin"]:
            with self.subTest(subject=subject):
                review = approved(row, subject=subject)
                decision = evaluate(row, review)
                self.assertEqual("mechanism_only", decision.status)
                with self.assertRaises(ValueError):
                    candidate_edge(row, review, decision)

    def test_fail_closed_fields(self):
        row = source()
        for changes in [
            {"source_verified": "true"}, {"schema_version": "old"}, {"reviewer": ""},
            {"reviewed_on": "tomorrow"}, {"reviewed_on": "2999-01-01"},
            {"supporting_quote": "Not in source"}, {"source_locator": ""},
            {"subject": "unknown"}, {"route": "oral"}, {"route": "injection"},
            {"claim_kind": "safety"}, {"claim_kind": "mechanism"},
            {"attribution": "multi_active_combination"}, {"attribution": "product_only"},
            {"comparator": "baseline_only"}, {"comparator": "unknown"},
            {"effect_code": "ANTI_AGING"}, {"measured_endpoint": "product_adjective"},
            {"change_direction": "decrease"}, {"result_support": "refuted"},
            {"significance": "not_significant"}, {"significance": "unclear"},
            {"claim_entailment_verified": False}, {"population": ""},
            {"concentration": "unknown"}, {"limitations": []}, {"limitations": "text"},
            {"context_excerpt": ""}, {"context_source_url": "https://pubmed.ncbi.nlm.nih.gov/1/"},
        ]:
            with self.subTest(changes=changes):
                self.assertEqual("review_required", evaluate(row, approved(row, **changes)).status)

    def test_source_changes_invalidate_approval(self):
        row = source()
        review = approved(row)
        for key in ["source_sentence", "title", "ingredient_name", "pmid", "gold_batch_id"]:
            with self.subTest(key=key):
                self.assertEqual("review_required", evaluate({**row, key: "changed"}, review).status)

    def test_rejection_needs_a_source_bound_explanation(self):
        row = source()
        self.assertEqual("reject", evaluate(row, approved(row, decision="reject")).status)
        self.assertEqual("review_required", evaluate(row, approved(
            row, decision="reject", review_note=""
        )).status)

    def test_unverified_model_claim_is_not_promoted_to_explanation(self):
        row = source()
        self.assertEqual("review_required", evaluate(row, approved(
            row, subject="skin_cells", claim_entailment_verified=False
        )).status)

    def test_missing_source_identity(self):
        for changes in [{"pmid": ""}, {"pmid": "nan"}, {"pmid": "0"}, {"title": ""}]:
            row = source(**changes)
            self.assertEqual("review_required", evaluate(row, approved(row)).status)

    def test_review_cannot_be_reused_for_a_different_edge(self):
        row = source()
        review = approved(row)
        decision = evaluate(row, review)
        with self.assertRaises(ValueError):
            candidate_edge(row, {**review, "change_direction": "decrease"}, decision)


class ShadowAuditTest(unittest.TestCase):
    def write_claims(self, directory, rows):
        path = directory / "claims.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_audit_preserves_raw_multiline_source_and_deduplicates(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            row = source(source_sentence="First line.\nSecond line.")
            path = self.write_claims(root, [row, row])
            before = path.read_bytes()
            out = root / "audit"
            manifest = audit(path, out)
            self.assertEqual(1, manifest["duplicate_records"])
            self.assertEqual({"review_required": 1}, manifest["status_counts"])
            self.assertEqual("", (out / "candidate_edges.jsonl").read_text())
            saved = json.loads((out / "decisions.jsonl").read_text())
            self.assertEqual(row, saved["source"])
            self.assertEqual(before, path.read_bytes())
            with self.assertRaises(FileExistsError):
                audit(path, out)

    def test_approved_candidate_coverage_counts_unique_papers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [source(), source(evidence_id="synthetic-2")]
            path = self.write_claims(root, rows)
            reviews = root / "reviews.jsonl"
            reviews.write_text("\n".join(json.dumps(approved(row)) for row in rows))
            manifest = audit(path, root / "out", reviews)
            self.assertEqual({"candidate": 2}, manifest["status_counts"])
            self.assertEqual({"HYDRATING": 1}, manifest["candidate_unique_pmids_by_effect"])

    def test_utf8_bom_does_not_change_parsed_record_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            row = source()
            path = self.write_claims(root, [row])
            path.write_bytes(b"\xef\xbb\xbf" + path.read_bytes())
            audit(path, root / "out")
            saved = json.loads((root / "out" / "decisions.jsonl").read_text())
            self.assertEqual(record_id(row), saved["record_id"])

    def test_duplicate_and_stale_reviews_fail_before_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = self.write_claims(root, [source()])
            reviews = root / "reviews.jsonl"
            value = json.dumps(approved(source()))
            reviews.write_text(value + "\n" + value)
            with self.assertRaises(ValueError):
                load_reviews(reviews)
            reviews.write_text(json.dumps(approved(source(title="changed"))))
            with self.assertRaises(ValueError):
                audit(path, root / "out", reviews)
            self.assertFalse((root / "out").exists())


if __name__ == "__main__":
    unittest.main()
