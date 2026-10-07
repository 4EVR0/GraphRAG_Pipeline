import unittest
from pathlib import Path

from pipeline.review.context import classify_conditions, load_concern_conditions, score_concerns

CONFIG = Path(__file__).resolve().parents[1] / "config" / "review" / "concern_conditions.csv"


def _r(**kw) -> dict:
    base = {"pmid": "1", "ingredient_inci": "UREA", "effect_code": "KERATOLYTIC", "study_type": "rct",
            "population": "", "weight": 1.0}
    return {**base, **kw}


class ClassifyTest(unittest.TestCase):
    def test_conditions(self) -> None:
        self.assertEqual(classify_conditions(_r(population="40 adults with acne vulgaris")), {"acne"})
        self.assertEqual(classify_conditions(_r(population="25 men and women with moderate to severe xerosis")), {"dry"})
        self.assertEqual(classify_conditions(_r(population="10 patients (20 feet) with diabetic foot syndrome")),
                         {"keratinization", "other_disease"})
        self.assertIn("atopic", classify_conditions(_r(population="children with atopic dermatitis")))
        self.assertNotIn("atopic", classify_conditions(_r(population="patients with seborrheic dermatitis")))
        self.assertEqual(classify_conditions(_r(population="", study_type="review"), "Melasma treatments"), {"pigment"})
        self.assertEqual(classify_conditions(_r(study_type="in_vitro")), {"nonhuman"})
        self.assertEqual(classify_conditions(_r(population="30 subjects")), {"unspecified"})


class ConcernScoreTest(unittest.TestCase):
    def test_keratolytic_from_xerosis_does_not_count_for_acne(self) -> None:
        table = load_concern_conditions(CONFIG)
        records = [
            _r(pmid="1", population="xerosis patients"),
            _r(pmid="2", population="acne vulgaris patients", ingredient_inci="SALICYLIC ACID", effect_code="BLEMISH_CARE"),
            _r(pmid="3", population="acne patients", ingredient_inci="SALICYLIC ACID", effect_code="COMEDOLYTIC", weight=0.6),
            _r(pmid="2", population="acne patients", ingredient_inci="SALICYLIC ACID", effect_code="COMEDOLYTIC", weight=0.5),
            _r(pmid="4", population="acne patients", weight=0.0),
            _r(pmid="5", study_type="in_vitro", ingredient_inci="SALICYLIC ACID", effect_code="BLEMISH_CARE"),
        ]
        marked, edges = score_concerns(records, table)
        self.assertEqual(marked[0]["conditions"], "dry")
        acne = {e["ingredient_inci"]: e for e in edges if e["concern_code"] == "ACNE"}
        self.assertNotIn("UREA", acne)
        self.assertEqual(acne["SALICYLIC ACID"]["paper_count"], 2)
        self.assertEqual(acne["SALICYLIC ACID"]["effects"], "BLEMISH_CARE|COMEDOLYTIC")
        self.assertIn(("UREA", "FLAKY_SKIN"), {(e["ingredient_inci"], e["concern_code"]) for e in edges})

    def test_config_covers_server_concerns(self) -> None:
        table = load_concern_conditions(CONFIG)
        self.assertEqual(len(table), 26)
        self.assertEqual(table["ACNE"][1], frozenset({"acne", "oily"}))
