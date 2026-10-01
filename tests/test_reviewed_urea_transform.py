"""Synthetic contracts only. Real article text and user records stay local."""
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.transform_reviewed_urea_pilot import CASES, PASSAGES, digest, run, transform


class UreaTransformTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.paths = {k: self.root / k for k in ("snapshot", "annotations", "supplement", "checklist", "acceptance")}
        self.parents = {}
        drafts = []
        for site, support, direction, significance in [("legs", "supported", "increase", "significant"),
                                                       ("arms", "no_detected_effect", "no_detected_difference", "not_significant")]:
            fields = {k: {"value": v} for k, v in dict(body_site=site, ingredient_name="Urea", measured_endpoint="skin_water_content",
                      result_support=support, change_direction=direction, significance=significance,
                      population="synthetic disease-specific population", concentration="synthetic concentration",
                      formulation="synthetic cream", duration="synthetic duration").items()}
            self.parents[site] = {"observation_id": site, "draft_sha256": site + "-draft", "source": {"pmid": "35663767"},
                                  "draft": {"fields": fields}}
            drafts.append({"pmid": "35663767", "site": site})
        self.paths["annotations"].write_text("\n".join(json.dumps(d) for d in drafts))
        passages = []
        for pid in sorted(PASSAGES):
            text = "Synthetic passage " + pid
            xml = "<p>" + text + "</p>"
            passages.append({"id": pid, "text": text, "text_sha256": digest(text.encode()),
                             "parsed_fragment_xml": xml, "fragment_sha256": digest(xml.encode()),
                             "section_id": "synthetic-" + pid, "section_title": "Synthetic", "paragraph_index_zero_based": 0})
        self.sup = {
            "schema_version": "local-selected-passage-supplement-v1", "pmid": "35663767", "pmcid_version": "PMC9060062.1",
            "doi": "10.1002/ski2.65", "source_url": "https://pmc.ncbi.nlm.nih.gov/articles/PMC9060062/",
            "license": "CC BY 4.0", "license_url": "https://creativecommons.org/licenses/by/4.0/",
            "parent_annotations_sha256": digest(self.paths["annotations"].read_bytes()), "parent_abstract_response_sha256": "synthetic-abstract",
            "parent_observations": [{"body_site": s, "observation_id": s, "draft_sha256": s + "-draft"} for s in self.parents],
            "selected_passages": passages, "recommendation_eligible": False, "response_sha256": "synthetic-fulltext",
            "proposed_context_updates": {k: {"value": v, "passage_ids": ["same_base"]} for k, v in
                {"study_control": "matched_vehicle", "attribution": "isolated_ingredient", "result_comparator": "matched_vehicle"}.items()},
            "interpretation": {"statistical_reporting_note": "Synthetic note; preserve divergent reports."},
        }
        items = []
        for case, (classification, site, *_rest) in CASES.items():
            refs = ["tewl_excluded"] if site is None else ["same_base", "legs_hydration_difference" if site == "legs" else "arms_no_detected_difference"]
            items.append({"id": case, "proposed_classification": classification, "source_passages": refs,
                          "forbidden_generalizations": ["No clinical use; synthetic"],
                          "source_locations": [{k: p[k] for k in ("id", "section_id", "section_title", "paragraph_index_zero_based", "text_sha256")}
                                               for p in passages if p["id"] in refs]})
        self.check = {"version": "urea-human-review-checklist-v1", "pmid": "35663767", "pmcid_version": "PMC9060062.1", "cases": items}
        self.accept = {"schema_version": "user-classification-acceptance-v1", "reviewer": "synthetic-reviewer",
                       "production_recommendation_approved": False, "clinical_validity_review_completed": False,
                       "is_gate_approval_file": False, "independent_full_source_read_confirmed": False,
                       "cases": [{"id": key, "decision": "accept_presented_classification", "classification": val[0]} for key, val in CASES.items()]}
        self.write_inputs()
        # The snapshot and draft validators have their own tests. This unit test
        # isolates the new supplement/acceptance transformation contract.
        self.addCleanup(patch.stopall)
        patch("scripts.transform_reviewed_urea_pilot.load_snapshot", return_value=({"response_sha256": "synthetic-abstract"}, {"35663767": {}})).start()
        patch("scripts.transform_reviewed_urea_pilot.make_packet", side_effect=lambda d, *_: self.parents[d["site"]]).start()

    def write_inputs(self):
        self.paths["supplement"].write_text(json.dumps(self.sup))
        self.check["source_supplement_sha256"] = digest(self.paths["supplement"].read_bytes())
        self.paths["checklist"].write_text(json.dumps(self.check))
        self.accept["source_supplement_sha256"] = self.check["source_supplement_sha256"]
        self.accept["review_checklist_sha256"] = digest(self.paths["checklist"].read_bytes())
        self.paths["acceptance"].write_text(json.dumps(self.accept))

    def test_three_states_and_scopes_survive_without_production_approval(self):
        outcomes, hashes = transform(**self.paths)
        legs, arms, excluded = outcomes
        self.assertEqual("HYDRATING", legs["effect_code"])
        self.assertEqual("legs", legs["constraints"]["body_site"]["value"])
        self.assertEqual("arms", arms["constraints"]["body_site"]["value"])
        self.assertIsNone(arms["effect_code"])
        self.assertEqual("no_detected_effect", arms["result_support"])
        self.assertEqual("excluded_not_reported", excluded["result_support"])
        for key in ("change_direction", "significance", "effect_code", "constraints", "parent_observation"):
            self.assertIsNone(excluded[key])
        for row in outcomes:
            self.assertFalse(row["recommendation_eligible"])
            self.assertFalse(row["clinical_validity_review_completed"])
            self.assertFalse(row["independent_full_source_read_confirmed"])
            self.assertEqual(hashes, row["input_sha256"])
            self.assertNotIn("graph_score", row)
        self.assertEqual(3, len({r["outcome_id"] for r in outcomes}))

    def test_fulltext_does_not_replace_abstract_citations(self):
        outcomes, _ = transform(**self.paths)
        self.assertEqual(self.parents["legs"], outcomes[0]["parent_observation"])
        self.assertEqual("PMC9060062.1", outcomes[0]["context_supplement"]["pmcid_version"])
        self.assertTrue(outcomes[0]["selected_passages"])

    def test_supplement_updates_do_not_carry_other_sites_result_reference(self):
        self.sup["proposed_context_updates"]["result_comparator"]["passage_ids"] = [
            "same_base", "arms_no_detected_difference", "legs_hydration_difference"]
        self.write_inputs()
        outcomes, _ = transform(**self.paths)
        for row, other_site in [(outcomes[0], "arms_no_detected_difference"), (outcomes[1], "legs_hydration_difference")]:
            selected = {p["id"] for p in row["selected_passages"]}
            for update in row["context_supplement"]["updates"].values():
                self.assertTrue(set(update["passage_ids"]).issubset(selected))
                self.assertNotIn(other_site, update["passage_ids"])

    def test_source_change_after_acceptance_is_rejected(self):
        self.paths["supplement"].write_text(self.paths["supplement"].read_text() + " ")
        with self.assertRaises(ValueError):
            transform(**self.paths)

    def test_omitted_duplicate_or_unaccepted_cases_are_rejected(self):
        original = copy.deepcopy(self.accept)
        for change in [lambda x: x["cases"].pop(), lambda x: x["cases"].append(x["cases"][0]),
                       lambda x: x["cases"][0].update(decision="pending"),
                       lambda x: x["cases"][2].update(classification="conditional_positive_hydration_evidence")]:
            self.accept = copy.deepcopy(original)
            change(self.accept)
            self.write_inputs()
            with self.assertRaises(ValueError):
                transform(**self.paths)

    def test_changed_body_site_or_result_cannot_be_promoted(self):
        self.parents["legs"]["draft"]["fields"]["body_site"]["value"] = "face"
        with self.assertRaises(ValueError):
            transform(**self.paths)
        self.parents["legs"]["draft"]["fields"]["body_site"]["value"] = "legs"
        self.parents["arms"]["draft"]["fields"]["result_support"]["value"] = "supported"
        with self.assertRaises(ValueError):
            transform(**self.paths)

    def test_excluded_tewl_requires_its_own_source(self):
        self.check["cases"][2]["source_passages"] = ["legs_hydration_difference"]
        self.write_inputs()
        with self.assertRaises(ValueError):
            transform(**self.paths)

    def test_xml_projection_mismatch_rejected_even_with_updated_hashes(self):
        self.sup["selected_passages"][0]["text"] = "Mismatched projection"
        self.sup["selected_passages"][0]["text_sha256"] = digest(b"Mismatched projection")
        self.write_inputs()
        with self.assertRaises(ValueError):
            transform(**self.paths)

    def test_production_approval_input_not_accepted(self):
        self.accept["production_recommendation_approved"] = True
        self.write_inputs()
        with self.assertRaises(ValueError):
            transform(**self.paths)

    def test_originals_preserved_new_directory_only_and_invalid_no_output(self):
        before = {k: p.read_bytes() for k, p in self.paths.items() if k != "snapshot"}
        out = self.root / "output"
        result = run(**self.paths, output=out)
        self.assertEqual(3, result["outcome_count"])
        self.assertEqual(0, result["recommendation_eligible_count"])
        self.assertEqual(before, {k: p.read_bytes() for k, p in self.paths.items() if k != "snapshot"})
        with self.assertRaises(ValueError):
            run(**self.paths, output=out)
        self.accept["cases"] = []
        self.write_inputs()
        with self.assertRaises(ValueError):
            run(**self.paths, output=self.root / "invalid")
        self.assertFalse((self.root / "invalid").exists())


if __name__ == "__main__":
    unittest.main()
