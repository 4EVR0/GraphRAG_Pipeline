import csv
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TARGET_CSV = ROOT / "config" / "target_ingredients.csv"


def _load_targets() -> list[dict[str, str]]:
    with TARGET_CSV.open(newline="", encoding="utf-8-sig") as file:
        return [
            row
            for row in csv.DictReader(file)
            if row.get("is_target", "").strip().lower() == "true"
        ]


def _keyword_set(row: dict[str, str]) -> set[str]:
    return {
        keyword.strip().lower()
        for keyword in row.get("concern_keywords", "").split("|")
        if keyword.strip()
    }


class TargetIngredientCoverageTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rows = _load_targets()
        cls.by_query_name = {row["query_name"].lower(): row for row in cls.rows}

    def assertIngredientCovers(
        self,
        query_name: str,
        *,
        category: str | None = None,
        keywords: set[str],
    ) -> None:
        row = self.by_query_name[query_name.lower()]
        if category is not None:
            self.assertEqual(category, row["category"])
        self.assertTrue(
            keywords <= _keyword_set(row),
            f"{query_name} is missing {sorted(keywords - _keyword_set(row))}",
        )

    def test_keratolytic_targets_have_direct_search_hints(self) -> None:
        required = {"exfoliation", "keratolytic", "skin renewal"}

        for query_name in [
            "Salicylic Acid",
            "Betaine Salicylate",
            "Glycolic Acid",
            "Lactic Acid",
            "Mandelic Acid",
            "Gluconolactone",
            "Urea",
        ]:
            with self.subTest(query_name=query_name):
                self.assertIngredientCovers(
                    query_name,
                    category="exfoliation",
                    keywords=required,
                )

    def test_sebum_targets_have_direct_search_hints(self) -> None:
        required = {"sebum", "oiliness", "oily skin", "pore"}

        for query_name in ["Niacinamide", "Zinc PCA", "Zinc Gluconate", "Azelaic Acid"]:
            with self.subTest(query_name=query_name):
                self.assertIngredientCovers(query_name, keywords=required)

    def test_dullness_targets_have_brightening_search_hints(self) -> None:
        required = {"brightening", "hyperpigmentation", "dullness", "uneven skin tone"}

        for query_name in [
            "Ascorbic Acid",
            "3-O-Ethyl Ascorbic Acid",
            "Sodium Ascorbyl Phosphate",
            "Ascorbyl Glucoside",
            "Tranexamic Acid",
            "Arbutin",
        ]:
            with self.subTest(query_name=query_name):
                self.assertIngredientCovers(
                    query_name,
                    category="brightening",
                    keywords=required,
                )

    def test_undercovered_efficacy_terms_have_minimum_target_counts(self) -> None:
        term_counts = {
            term: sum(1 for row in self.rows if term in _keyword_set(row))
            for term in ["keratolytic", "sebum", "dullness"]
        }

        self.assertGreaterEqual(term_counts["keratolytic"], 8)
        self.assertGreaterEqual(term_counts["sebum"], 10)
        self.assertGreaterEqual(term_counts["dullness"], 10)


if __name__ == "__main__":
    unittest.main()
