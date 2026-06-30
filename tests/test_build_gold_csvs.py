import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from scripts import build_gold_csvs


class ClaimBatchSelectionTest(unittest.TestCase):
    def test_selects_exact_claim_batch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            expected = root / "batch=full-v5"
            expected.mkdir()

            with patch.object(build_gold_csvs, "CLAIM_BATCH_ROOT", root):
                batches = build_gold_csvs._all_claim_batches(
                    claim_batch_id="full-v5"
                )

            self.assertEqual([expected], batches)

    def test_rejects_conflicting_batch_filters(self) -> None:
        with self.assertRaises(ValueError):
            build_gold_csvs._all_claim_batches(
                since="2026-06-30",
                claim_batch_id="full-v5",
            )

    def test_affects_scores_do_not_leak_between_effects(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            batch = root / "batch=full-v5"
            batch.mkdir()
            rows = [
                {
                    "ingredient_name": "Niacinamide",
                    "relation": "improves",
                    "effect_ids": "1",
                    "concern_ids": "",
                    "eligibility_tier": "soft_graph",
                    "strength_label": "strong",
                    "significance_label": "significant",
                    "attribution_label": "single_active",
                    "claim_type": "efficacy",
                    "source_sentence": "Niacinamide improved skin hydration.",
                    "title": "",
                    "study_context": "human_topical",
                    "all_detected_ingredients": "Niacinamide",
                    "pmid": "100",
                    "row_weight": "0.5",
                },
                {
                    "ingredient_name": "Niacinamide",
                    "relation": "improves",
                    "effect_ids": "2",
                    "concern_ids": "",
                    "eligibility_tier": "soft_graph",
                    "strength_label": "strong",
                    "significance_label": "significant",
                    "attribution_label": "single_active",
                    "claim_type": "efficacy",
                    "source_sentence": "Niacinamide improved skin texture.",
                    "title": "",
                    "study_context": "human_topical",
                    "all_detected_ingredients": "Niacinamide",
                    "pmid": "200",
                    "row_weight": "0.25",
                },
            ]
            with (batch / "gold_claim_all.csv").open(
                "w", encoding="utf-8", newline=""
            ) as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)

            with patch.object(build_gold_csvs, "CLAIM_BATCH_ROOT", root):
                edges = build_gold_csvs.load_affects_rows(
                    {1: "HYDRATING", 2: "KERATOLYTIC"},
                    {"niacinamide": "NIACINAMIDE"},
                    claim_batch_id="full-v5",
                )

            by_effect = {row[":END_ID(Effect)"]: row for row in edges}
            self.assertEqual(1, by_effect["HYDRATING"]["paper_count:int"])
            self.assertEqual(1, by_effect["KERATOLYTIC"]["paper_count:int"])
            self.assertGreater(
                by_effect["HYDRATING"]["graph_score:float"],
                by_effect["KERATOLYTIC"]["graph_score:float"],
            )

    def test_cosing_edges_only_reference_product_ingredients(self) -> None:
        products = pd.DataFrame(
            [{"inci_name": "IN PRODUCT", "cosing_functions": "HUMECTANT"}]
        )
        inci = pd.DataFrame(
            [
                {"inci_name": "IN PRODUCT", "cosing_functions": "HUMECTANT"},
                {"inci_name": "NOT IN PRODUCT", "cosing_functions": "HUMECTANT"},
            ]
        )

        edges = build_gold_csvs.build_cosing_soft_edges(
            products,
            inci,
            pubmed_seen=set(),
            valid_effects={"HYDRATING", "MOISTURE_RETENTION"},
        )

        self.assertEqual(
            {"IN PRODUCT"},
            {row[":START_ID(Ingredient)"] for row in edges},
        )


if __name__ == "__main__":
    unittest.main()
