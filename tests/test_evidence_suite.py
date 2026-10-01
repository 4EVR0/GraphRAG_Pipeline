import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.verify_evidence_suite import VERSION, compare_packets, run, sha


class EvidenceSuiteTest(unittest.TestCase):
    def setUp(self):
        self.packet = {"observation_id": "synthetic", "source": {"pmid": "123"},
                       "draft": {"fields": {"result_support": {"value": "supported"}, "body_site": None}},
                       "proposed_effect_code": "HYDRATING", "pre_review_blockers": ["missing:body_site"],
                       "recommendation_eligible": False, "status": "pending_independent_review"}
        self.expected = {"pmid": "123", "values": {"result_support": "supported", "body_site": None},
                         "effect": "HYDRATING", "required_blockers": ["missing:body_site"]}

    def test_positive_finding_retained_without_approval(self):
        before = copy.deepcopy(self.packet)
        self.assertTrue(compare_packets([self.packet], [self.expected])[0]["passed"])
        self.assertEqual(before, self.packet)

    def test_dropped_positive_wrong_effect_or_approval_fails(self):
        for patch_values in ({"proposed_effect_code": None}, {"proposed_effect_code": "BARRIER_REPAIR"},
                             {"recommendation_eligible": True}, {"status": "approved"},
                             {"pre_review_blockers": []}, {"source": {"pmid": "456"}}):
            with self.subTest(patch=patch_values):
                self.assertFalse(compare_packets([{**self.packet, **patch_values}], [self.expected])[0]["passed"])

    def test_unknown_condition_cannot_be_filled_or_polarity_reversed(self):
        for key, value in (("body_site", "face"), ("result_support", "no_detected_effect")):
            packet = copy.deepcopy(self.packet)
            packet["draft"]["fields"][key] = {"value": value}
            self.assertFalse(compare_packets([packet], [self.expected])[0]["passed"])

    def test_empty_or_incomplete_inventory_fails(self):
        for packets, expected in (([], []), ([self.packet], []), ([], [self.expected]), ([self.packet], [{}])):
            with self.assertRaises(ValueError):
                compare_packets(packets, expected)

    def test_end_to_end_report_and_input_guards(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = {k: root / k for k in ("claims", "reviews", "atomic_annotations", "annotations",
                                        "supplement", "checklist", "acceptance", "expectations", "output")}
            for key in ("atomic_snapshot", "snapshot"):
                args[key] = root / key
                args[key].mkdir()
                for name in ("pubmed.xml", "manifest.json"):
                    (args[key] / name).write_text("synthetic")
            for key in ("claims", "supplement", "checklist", "acceptance"):
                args[key].write_text("synthetic")
            args["reviews"].write_text(json.dumps({"record_id": "r", "decision": "reject"}) + "\n")
            for key in ("atomic_annotations", "annotations"):
                args[key].write_text(json.dumps(self.packet["draft"]) + "\n")
            inputs = {k: args[k] for k in ("claims", "reviews", "atomic_annotations", "annotations", "supplement", "checklist", "acceptance")}
            inputs.update(atomic_xml=args["atomic_snapshot"] / "pubmed.xml", control_xml=args["snapshot"] / "pubmed.xml",
                          atomic_manifest=args["atomic_snapshot"] / "manifest.json", control_manifest=args["snapshot"] / "manifest.json")
            spec = {"version": VERSION, "input_sha256": {k: sha(p) for k, p in inputs.items()},
                    "packets": {"atomic": [self.expected], "controls": [self.expected]},
                    "rejected_record_ids": ["r"], "legacy_unique_records": 2}
            args["expectations"].write_text(json.dumps(spec))

            def build_fixture(source, drafts, output):
                output.mkdir()
                (output / "review_packets.jsonl").write_text(json.dumps(self.packet) + "\n")

            def audit_fixture(claims, output, reviews=None):
                output.mkdir()
                rows = [{"record_id": rid, "source": {"id": rid},
                         "status": "reject" if reviews and rid == "r" else "review_required"} for rid in ("r", "p")]
                (output / "decisions.jsonl").write_text("\n".join(json.dumps(row) for row in rows))
                (output / "candidate_edges.jsonl").write_text("")
                return {"unique_records": 2, "status_counts": {"review_required": 2}}

            def urea_fixture(**kwargs):
                kwargs["output"].mkdir()
                (kwargs["output"] / "applicability_report.json").write_text(json.dumps({"checks": [{"passed": True}]}))
                return {"code_sha": "synthetic", "code_dirty": False}

            with patch("scripts.verify_evidence_suite.build", side_effect=build_fixture), \
                 patch("scripts.verify_evidence_suite.audit", side_effect=audit_fixture), \
                 patch("scripts.verify_evidence_suite.verify_urea", side_effect=urea_fixture):
                report = run(**args)
                self.assertEqual(0, report["failed"])
                self.assertFalse(report["production_ready"])
                with self.assertRaises(FileExistsError):
                    run(**args)
                args["output"] = root / "failed"
                spec["packets"]["controls"][0] = {**self.expected, "effect": None}
                args["expectations"].write_text(json.dumps(spec))
                self.assertEqual(1, run(**args)["failed"])
                args["output"] = root / "invalid"
                args["claims"].write_text("changed")
                with self.assertRaises(ValueError):
                    run(**args)
                self.assertFalse(args["output"].exists())


if __name__ == "__main__":
    unittest.main()
