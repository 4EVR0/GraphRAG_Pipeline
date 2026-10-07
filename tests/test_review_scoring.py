import math
import tempfile
import unittest
from pathlib import Path

from pipeline.review.scoring import (
    formulation_role,
    judgment_weight,
    load_cosing_functions,
    load_mfds_functional,
    score_records,
)


def _record(**overrides) -> dict:
    base = {
        "pmid": "1", "ingredient_inci": "SALICYLIC ACID", "effect_code": "BLEMISH_CARE",
        "attribution": "single", "route": "topical_leave_on", "study_type": "rct",
        "comparison": "placebo_or_vehicle", "significance": "significant", "direction": "improves",
        "sample_size": 40, "quote_verified": True, "values_valid": True, "effect_in_quote": True,
    }
    return {**base, **overrides}


class WeightTest(unittest.TestCase):
    def test_best_evidence_gets_full_weight(self) -> None:
        self.assertEqual(judgment_weight(_record()), (1.0, []))

    def test_exclusions_are_zero_with_reason(self) -> None:
        cases = {
            "unverified": _record(quote_verified=False),
            "direction=no_difference": _record(direction="no_difference"),
            "route=peel": _record(route="peel"),
            "route=procedure": _record(route="procedure"),
            "not_significant": _record(significance="not_significant"),
        }
        for reason, record in cases.items():
            self.assertEqual(judgment_weight(record), (0.0, [reason]))

    def test_weaker_designs_weigh_less(self) -> None:
        self.assertAlmostEqual(judgment_weight(_record(attribution="combination"))[0], 0.35)
        self.assertAlmostEqual(judgment_weight(_record(study_type="in_vitro", route="not_applicable"))[0], 0.1)
        self.assertAlmostEqual(judgment_weight(_record(comparison="baseline_only"))[0], 0.6)

    def test_v1_records_without_strength_fields_are_not_reported(self) -> None:
        record = _record()
        for key in ("comparison", "significance", "sample_size", "effect_in_quote"):
            record.pop(key)
        self.assertAlmostEqual(judgment_weight(record)[0], 0.6 * 0.7)

    def test_penalties_multiply_and_are_listed(self) -> None:
        weight, reasons = judgment_weight(_record(effect_in_quote=False, sample_size=12), {"VISCOSITY CONTROLLING"})
        self.assertAlmostEqual(weight, 0.5 * 0.5 * 0.8)
        self.assertEqual(reasons, ["effect_not_in_quote", "formulation_role", "small_study"])


class FormulationRoleTest(unittest.TestCase):
    def test_only_formulation_functions_count(self) -> None:
        self.assertTrue(formulation_role({"VISCOSITY CONTROLLING", "SKIN CONDITIONING", "GEL FORMING"}))
        self.assertTrue(formulation_role({"SOLVENT", "SKIN CONDITIONING"}))
        # 보습제는 용제를 겸해도 제형 역할 성분이 아니다.
        self.assertFalse(formulation_role({"HUMECTANT", "SOLVENT", "VISCOSITY CONTROLLING"}))
        self.assertFalse(formulation_role({"KERATOLYTIC", "PRESERVATIVE"}))
        self.assertFalse(formulation_role({"SKIN CONDITIONING"}))
        self.assertFalse(formulation_role(None))


class AggregateTest(unittest.TestCase):
    def test_paper_counted_once_with_max_weight(self) -> None:
        records = [
            _record(pmid="1"),
            _record(pmid="1", attribution="combination"),
            _record(pmid="2", study_type="cohort"),
            _record(pmid="3", direction="worsens"),
            _record(pmid="4", effect_code="HYDRATING", study_type="in_vitro"),
        ]
        scored, edges = score_records(records)
        self.assertEqual([r["weight"] for r in scored], [1.0, 0.35, 0.6, 0.0, 0.1])
        blemish = next(e for e in edges if e["effect_code"] == "BLEMISH_CARE")
        self.assertAlmostEqual(blemish["score"], round(math.log1p(1.6), 6))
        self.assertEqual((blemish["paper_count"], blemish["human_paper_count"]), (2, 2))
        self.assertEqual(blemish["top_pmids"], "1|2")
        hydrating = next(e for e in edges if e["effect_code"] == "HYDRATING")
        self.assertEqual(hydrating["human_paper_count"], 0)

    def test_mfds_acne_condition_is_flagged(self) -> None:
        mfds = {("SALICYLIC ACID", "BLEMISH_CARE"): {"function": "acne", "max_content": "2%", "condition": ""}}
        scored, edges = score_records([_record(), _record(pmid="2", route="topical_rinse_off")], mfds=mfds)
        self.assertEqual([r["flags"] for r in scored], ["mfds_acne|mfds_route_mismatch", "mfds_acne"])
        self.assertTrue(edges[0]["mfds_functional"])


class LoaderTest(unittest.TestCase):
    def test_loaders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cosing = Path(tmp) / "gold.csv"
            cosing.write_text("inci_name,cosing_functions\nCARBOMER,EMULSION STABILISING;GEL FORMING\n"
                              "GLYCERIN,HUMECTANT;SOLVENT\n", encoding="utf-8")
            mfds = Path(tmp) / "mfds.csv"
            mfds.write_text("inci_name,function,max_content,condition,effect_codes\n"
                            "NIACINAMIDE,whitening,2～5%,,BRIGHTENING|DEPIGMENTING\n,whitening,2%,,DEPIGMENTING\n",
                            encoding="utf-8")
            functions = load_cosing_functions(cosing)
            table = load_mfds_functional(mfds)
        self.assertTrue(formulation_role(functions["CARBOMER"]))
        self.assertFalse(formulation_role(functions["GLYCERIN"]))
        self.assertEqual(set(table), {("NIACINAMIDE", "BRIGHTENING"), ("NIACINAMIDE", "DEPIGMENTING")})


if __name__ == "__main__":
    unittest.main()
