import unittest
from pathlib import Path

from pipeline.review.context import classify_conditions, load_concern_conditions, load_sensitive_cautions, score_concerns

CONFIG = Path(__file__).resolve().parents[1] / "config" / "review" / "concern_conditions.csv"
CAUTIONS = CONFIG.with_name("sensitive_skin_cautions.csv")


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

    def test_sensitive_is_separate_from_induced_irritation(self) -> None:
        self.assertEqual(classify_conditions(_r(population="60 women with sensitive, mildly photodamaged skin")),
                         {"sensitive", "aging"})
        self.assertEqual(classify_conditions(_r(population="patients with rosacea")), {"sensitive"})
        self.assertEqual(classify_conditions(_r(population="healthy volunteers, UV-induced erythema")),
                         {"healthy", "irritation", "photo"})


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

    def test_sensitive_skin_means_usable_on_sensitive_skin(self) -> None:
        table = load_concern_conditions(CONFIG)
        cautions = load_sensitive_cautions(CAUTIONS)
        records = [
            _r(pmid="1", ingredient_inci="PANTHENOL", effect_code="SOOTHING", population="subjects with sensitive skin"),
            _r(pmid="2", ingredient_inci="GLYCERIN", effect_code="HYDRATING", population="healthy volunteers"),
            _r(pmid="3", ingredient_inci="MENTHOL", effect_code="SOOTHING", population="subjects with sensitive skin"),
            _r(pmid="4", ingredient_inci="UREA", effect_code="HYDRATING", population="children with atopic dermatitis"),
        ]
        _, edges = score_concerns(records, table, cautions=cautions)
        pairs = {(e["ingredient_inci"], e["concern_code"]) for e in edges}
        self.assertIn(("PANTHENOL", "SENSITIVE_SKIN"), pairs)
        self.assertNotIn(("GLYCERIN", "SENSITIVE_SKIN"), pairs)
        self.assertNotIn(("MENTHOL", "SENSITIVE_SKIN"), pairs)
        self.assertNotIn(("UREA", "SENSITIVE_SKIN"), pairs)
        # 주의 목록은 민감 계열 고민에만 적용된다.
        self.assertIn(("UREA", "ATOPIC_PRONE"), pairs)

    def test_cautions_have_reason_and_source(self) -> None:
        import csv
        with open(CAUTIONS, encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertTrue(rows)
        for row in rows:
            self.assertTrue(row["reason"].strip() and row["source"].strip(), row["inci_name"])

    def test_config_covers_server_concerns(self) -> None:
        table = load_concern_conditions(CONFIG)
        self.assertEqual(len(table), 26)
        self.assertEqual(table["ACNE"][1], frozenset({"acne", "oily"}))
