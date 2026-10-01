import copy
import unittest

from pipeline.gold.claim.evidence_gate import record_id
from pipeline.gold.claim.shadow_applicability import assess
from scripts.verify_urea_applicability import BASE_CONTEXT, verify


def seal(row):
    row = {k: v for k, v in row.items() if k != "outcome_id"}
    return {**row, "outcome_id": record_id(row)}


def fixture():
    # Synthetic one-paper contract fixture, not a clinical approval.
    return seal({"transform_version": "reviewed-urea-shadow-v2", "pmid": "35663767",
        "case_id": "urea-legs-hydration", "use": "isolated_regression_fixture_not_production_evidence",
        "classification": "conditional_positive_hydration_evidence", "measured_endpoint": "skin_water_content",
        "result_support": "supported", "change_direction": "increase", "significance": "significant",
        "effect_code": "HYDRATING", "recommendation_eligible": False, "clinical_validity_review_completed": False,
        "applicability_scope": {k: v for k, v in BASE_CONTEXT.items() if k != "effect_code"}})


class ShadowApplicabilityTest(unittest.TestCase):
    def test_exact_context_is_pending_not_recommendation_approval(self):
        result = assess(fixture(), BASE_CONTEXT)
        self.assertTrue(result["context_match"])
        self.assertEqual("context_matched_review_pending", result["status"])
        self.assertFalse(result["evidence_use_allowed"])

    def test_each_missing_or_unknown_field_blocks(self):
        for key in BASE_CONTEXT:
            for omit in (True, False):
                query = dict(BASE_CONTEXT)
                if omit:
                    query.pop(key)
                else:
                    query[key] = None
                with self.subTest(key=key, omit=omit):
                    self.assertFalse(assess(fixture(), query)["context_match"])

    def test_no_alias_substring_wildcard_or_wrong_effect_matching(self):
        for patch in [{"population": "healthy_skin"}, {"body_site": "face"}, {"body_site": "legs or face"},
                      {"ingredient_name": "*"}, {"effect_code": "MOISTURE_RETENTION"}, {"effect_code": "BARRIER_REPAIR"},
                      {"formulation": "any cream"}, {"route": "oral"}, {"duration_days": 7}]:
            result = assess(fixture(), {**BASE_CONTEXT, **patch})
            self.assertFalse(result["context_match"])
            self.assertTrue(any(x.startswith("mismatch:") for x in result["reasons"]))

    def test_invalid_number_types_never_match(self):
        for key in ("concentration_percent", "duration_days"):
            for value in (True, "7.5", float("nan"), float("inf"), -1, 0):
                self.assertFalse(assess(fixture(), {**BASE_CONTEXT, key: value})["context_match"])

    def test_outcome_tampering_or_broadened_profile_blocked(self):
        row = fixture()
        row["applicability_scope"]["body_site"] = "face"
        self.assertIn("outcome_integrity_mismatch", assess(row, BASE_CONTEXT)["reasons"])
        self.assertIn("unsupported_study_scope", assess(seal(row), {**BASE_CONTEXT, "body_site": "face"})["reasons"])

    def test_excluded_and_no_difference_do_not_become_positive(self):
        for support, reason in [("excluded_not_reported", "outcome_excluded_not_reported"),
                                ("no_detected_effect", "no_positive_between_treatment_result")]:
            row = seal({**fixture(), "result_support": support, "effect_code": None})
            result = assess(row, BASE_CONTEXT)
            self.assertEqual([reason], result["reasons"])
            self.assertFalse(result["evidence_use_allowed"])

    def test_injected_approval_unknown_version_and_bad_queries_blocked(self):
        for patch in ({"recommendation_eligible": True}, {"clinical_validity_review_completed": True},
                      {"transform_version": "old"}, {"change_direction": "decrease"}):
            self.assertFalse(assess(seal({**fixture(), **patch}), BASE_CONTEXT)["context_match"])
        for query in (None, [], {**BASE_CONTEXT, "ignore_limits": True}):
            self.assertFalse(assess(fixture(), query)["context_match"])

    def test_matrix_checks_all_three_records_and_does_not_mutate(self):
        rows = [fixture(), seal({**fixture(), "case_id": "urea-arms-hydration", "result_support": "no_detected_effect", "effect_code": None}),
                seal({**fixture(), "case_id": "urea-tewl-excluded", "result_support": "excluded_not_reported", "effect_code": None})]
        before = copy.deepcopy(rows)
        checks = verify(rows)
        self.assertEqual(42, len(checks))
        self.assertTrue(all(c["passed"] for c in checks))
        self.assertEqual(rows, before)
        with self.assertRaises(ValueError):
            verify(rows[:1])


if __name__ == "__main__":
    unittest.main()
