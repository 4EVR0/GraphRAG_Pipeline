import csv
import math
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

    def test_suspect_ingredient_detection_does_not_support_an_edge(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            batch = root / "batch=suspect"
            batch.mkdir()
            base = {
                "ingredient_name": "Niacinamide", "relation": "improves",
                "effect_ids": "1", "concern_ids": "", "eligibility_tier": "soft_graph",
                "strength_label": "strong", "significance_label": "significant",
                "attribution_label": "single_active", "claim_type": "efficacy",
                "source_sentence": "Niacinamide improved skin hydration.",
                "title": "", "study_context": "human_topical",
                "all_detected_ingredients": "Niacinamide",
            }
            rows = [
                {**base, "pmid": "100", "row_weight": "0.5", "ingredient_detection_suspect": "True"},
                {**base, "pmid": "200", "row_weight": "0.25", "ingredient_detection_suspect": "False"},
            ]
            with (batch / "gold_claim_all.csv").open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)

            with patch.object(build_gold_csvs, "CLAIM_BATCH_ROOT", root):
                edges = build_gold_csvs.load_affects_rows(
                    {1: "HYDRATING"}, {"niacinamide": "NIACINAMIDE"},
                    claim_batch_id="suspect",
                )

            self.assertEqual(1, len(edges))
            self.assertEqual(1, edges[0]["paper_count:int"])
            self.assertEqual(round(math.log1p(0.25), 6), edges[0]["graph_score:float"])

    def test_ceramide_family_name_is_not_mapped_to_ceramide_np(self) -> None:
        inci = pd.DataFrame([{
            "inci_name": "CERAMIDE NP", "eng_name": "Ceramide", "kor_name": "세라마이드",
        }])
        lookup = build_gold_csvs.build_inci_lookup(inci, {"CERAMIDE NP"})

        self.assertEqual("CERAMIDE NP", lookup["ceramide np"])
        self.assertNotIn("ceramide", lookup)
        self.assertNotIn("세라마이드", lookup)

    def test_verified_peptide_name_does_not_alias_a_different_inci(self) -> None:
        inci = pd.DataFrame([
            {"inci_name": "ACETYL HEXAPEPTIDE-8", "eng_name": "Acetyl Hexapeptide-8",
             "kor_name": "아세틸헥사펩타이드-24아마이드"},
            {"inci_name": "ACETYL HEXAPEPTIDE-24 AMIDE", "eng_name": "Acetyl Hexapeptide-24 Amide",
             "kor_name": "아세틸헥사펩타이드-24아마이드"},
        ])

        corrected = build_gold_csvs.correct_verified_ingredient_names(inci)
        lookup = build_gold_csvs.build_inci_lookup(
            corrected, set(corrected["inci_name"]),
        )

        self.assertEqual("아세틸헥사펩타이드-8", corrected.iloc[0]["kor_name"])
        self.assertEqual("아세틸헥사펩타이드-24아마이드", inci.iloc[0]["kor_name"])
        self.assertEqual("ACETYL HEXAPEPTIDE-8", lookup["아세틸헥사펩타이드-8"])
        self.assertEqual("ACETYL HEXAPEPTIDE-24 AMIDE", lookup["아세틸헥사펩타이드-24아마이드"])

    def test_legacy_ceramide_np_paper_edges_are_not_restored(self) -> None:
        def edge(name: str, evidence_type: str) -> dict:
            return {
                ":START_ID(Ingredient)": name, ":END_ID(Effect)": "ANTI_AGING",
                "type": "improves", "evidence_type": evidence_type,
            }

        retained = build_gold_csvs.retain_legacy_affects(
            [
                edge("CERAMIDE NP", "pubmed_evidence"),
                edge("CERAMIDE NP", "cosing_function"),
                edge("NIACINAMIDE", "pubmed_evidence"),
            ],
            {"CERAMIDE NP", "NIACINAMIDE"}, {"ANTI_AGING"}, set(),
        )

        self.assertEqual(
            [edge("CERAMIDE NP", "cosing_function"), edge("NIACINAMIDE", "pubmed_evidence")],
            retained,
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

    def test_sebum_increase_is_not_emitted_as_an_affects_edge(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            batch = root / "batch=sebum"
            batch.mkdir()
            row = {
                "ingredient_name": "Niacinamide",
                "relation": "increases",
                "effect_ids": "6",
                "concern_ids": "",
                "eligibility_tier": "strict_graph",
                "strength_label": "strong",
                "significance_label": "significant",
                "attribution_label": "single_active",
                "claim_type": "efficacy",
                "source_sentence": "Niacinamide increased sebum.",
                "title": "",
                "study_context": "human_topical",
                "all_detected_ingredients": "Niacinamide",
                "pmid": "300",
                "row_weight": "0.5",
            }
            with (batch / "gold_claim_all.csv").open(
                "w", encoding="utf-8", newline=""
            ) as f:
                writer = csv.DictWriter(f, fieldnames=list(row))
                writer.writeheader()
                writer.writerow(row)

            with patch.object(build_gold_csvs, "CLAIM_BATCH_ROOT", root):
                edges = build_gold_csvs.load_affects_rows(
                    {6: "SEBUM_REGULATION"},
                    {"niacinamide": "NIACINAMIDE"},
                    claim_batch_id="sebum",
                )

            self.assertEqual([], edges)

    def _salicylic_edges(self, rows: list[dict]) -> list[dict]:
        base = {
            "ingredient_name": "Salicylic acid", "concern_ids": "",
            "eligibility_tier": "soft_graph", "strength_label": "moderate",
            "significance_label": "unclear", "attribution_label": "single_formulation",
            "claim_type": "efficacy", "title": "", "study_context": "human_topical",
            "all_detected_ingredients": "Salicylic acid",
        }
        rows = [{**base, **row} for row in rows]
        with tempfile.TemporaryDirectory() as temp_dir:
            batch = Path(temp_dir) / "batch=sa"
            batch.mkdir()
            with (batch / "gold_claim_all.csv").open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
            with patch.object(build_gold_csvs, "CLAIM_BATCH_ROOT", Path(temp_dir)):
                return build_gold_csvs.load_affects_rows(
                    {2: "SOOTHING", 3: "BARRIER_REPAIR", 4: "HYDRATING",
                     6: "SEBUM_REGULATION", 7: "KERATOLYTIC"},
                    {"salicylic acid": "SALICYLIC ACID"},
                    claim_batch_id="sa",
                )

    def test_one_sentence_does_not_spread_to_effects_outside_the_claim_target(self) -> None:
        sentence = (
            "CONCLUSION: The salicylic acid-containing gel effectively reduces acne "
            "lesions, regulates sebum production, enhances skin hydration, and "
            "strengthens the skin barrier."
        )
        edges = self._salicylic_edges([
            {"pmid": "40682377", "relation": "regulates", "target": "sebum production",
             "effect_ids": "4|6", "source_sentence": sentence, "row_weight": "0.27"},
            {"pmid": "40682377", "relation": "improves", "target": "hydration",
             "effect_ids": "2|3|4|6", "source_sentence": sentence, "row_weight": "0.27"},
        ])

        self.assertEqual(
            {("SEBUM_REGULATION", "regulates"), ("HYDRATING", "improves")},
            {(row[":END_ID(Effect)"], row["type"]) for row in edges},
        )

    def test_tolerability_is_not_emitted_as_an_efficacy_edge(self) -> None:
        sentence = "Salicylic acid is a tolerable alternative with mild peeling."
        edges = self._salicylic_edges([
            {"pmid": "41140645", "relation": "is_well_tolerated_for", "target": "tolerability",
             "effect_ids": "2|7", "source_sentence": sentence, "row_weight": "0.2"},
            {"pmid": "42145713", "relation": "improves", "target": "tolerability",
             "effect_ids": "2", "source_sentence": sentence, "row_weight": "0.27"},
        ])

        self.assertEqual([], edges)

    def test_relations_of_the_same_effect_share_one_edge(self) -> None:
        edges = self._salicylic_edges([
            {"pmid": "100", "relation": "reduces", "target": "sebum",
             "effect_ids": "6", "source_sentence": "Salicylic acid reduced sebum.",
             "row_weight": "0.27"},
            {"pmid": "100", "relation": "regulates", "target": "sebum production",
             "effect_ids": "6", "source_sentence": "Salicylic acid regulated sebum.",
             "row_weight": "0.27"},
            {"pmid": "200", "relation": "improves", "target": "oiliness",
             "effect_ids": "6", "source_sentence": "Salicylic acid improved oiliness.",
             "row_weight": "0.48"},
        ])

        self.assertEqual(1, len(edges))
        self.assertEqual("improves", edges[0]["type"])
        self.assertEqual(2, edges[0]["paper_count:int"])
        self.assertEqual(round(math.log1p(0.27 + 0.48), 6), edges[0]["graph_score:float"])

    def test_legacy_paper_edges_of_reevaluated_ingredients_are_not_restored(self) -> None:
        def edge(name: str, effect: str, evidence_type: str) -> dict:
            return {
                ":START_ID(Ingredient)": name, ":END_ID(Effect)": effect,
                "type": "regulates", "evidence_type": evidence_type,
            }

        retained = build_gold_csvs.retain_legacy_affects(
            [
                edge("SALICYLIC ACID", "HYDRATING", "pubmed_evidence"),
                edge("SALICYLIC ACID", "KERATOLYTIC", "cosing_function"),
                edge("NIACINAMIDE", "HYDRATING", "pubmed_evidence"),
            ],
            {"SALICYLIC ACID", "NIACINAMIDE"}, {"HYDRATING", "KERATOLYTIC"}, set(),
            {"SALICYLIC ACID"},
        )

        self.assertEqual(
            [
                edge("SALICYLIC ACID", "KERATOLYTIC", "cosing_function"),
                edge("NIACINAMIDE", "HYDRATING", "pubmed_evidence"),
            ],
            retained,
        )

    def _acne_edges(self, sentence: str, study_context: str = "human_topical",
                    target: str = "acne", title: str = "") -> set[tuple[str, int]]:
        effect_ids = {1: "ANTI_INFLAMMATORY", 3: "BARRIER_REPAIR", 4: "HYDRATING",
                      6: "SEBUM_REGULATION"}  # 운영처럼 claim_effect_map에 없는 효능은 빠져 있음
        row = {
            "ingredient_name": "Salicylic acid", "relation": "reduces", "target": target,
            "effect_ids": "3|4|6", "concern_ids": "", "eligibility_tier": "strict_graph",
            "strength_label": "moderate", "significance_label": "unclear",
            "attribution_label": "single_active", "claim_type": "efficacy",
            "source_sentence": sentence, "title": title, "study_context": study_context,
            "all_detected_ingredients": "Salicylic acid", "pmid": "1", "row_weight": "0.6",
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            batch = Path(temp_dir) / "batch=acne"
            batch.mkdir()
            with (batch / "gold_claim_all.csv").open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(row))
                writer.writeheader()
                writer.writerow(row)
            with patch.object(build_gold_csvs, "CLAIM_BATCH_ROOT", Path(temp_dir)):
                edges = build_gold_csvs.load_affects_rows(
                    effect_ids, {"salicylic acid": "SALICYLIC ACID"}, claim_batch_id="acne",
                )
        return {(row[":END_ID(Effect)"], row["paper_count:int"]) for row in edges}

    def test_acne_outcome_without_lesion_type_is_blemish_care(self) -> None:
        edges = self._acne_edges(
            "Photodynamic therapy combined with 2% salicylic acid reduced the number of "
            "skin lesions in patients with moderate acne, sebum and hydration."
        )

        self.assertEqual({("BLEMISH_CARE", 1)}, edges)

    def test_acne_lesion_types_map_to_mechanism_effects(self) -> None:
        self.assertEqual(
            {("ANTI_INFLAMMATORY", 1), ("COMEDOLYTIC", 1)},
            self._acne_edges(
                "Salicylic acid significantly reduced inflamed lesions after 1 month "
                "and non-inflamed lesions after 2 months in acne."
            ),
        )
        self.assertEqual(
            {("COMEDOLYTIC", 1)},
            self._acne_edges("Salicylic acid reduced comedones in acne patients."),
        )

    def test_acne_outcome_outside_human_acne_studies_is_not_an_edge(self) -> None:
        self.assertEqual(set(), self._acne_edges(
            "Zinc sulfate significantly reduced acne rosacea severity."))
        self.assertEqual(set(), self._acne_edges(
            "The gel reduced acne lesions in mice.", study_context="unknown"))
        self.assertEqual(set(), self._acne_edges(
            "Salicylic acid improved acne.", study_context="in_vitro"))
        self.assertEqual(set(), self._acne_edges(
            "Erythritol inhibited the growth of RTs associated with acne.",
            study_context="unknown",
            title="Ribotype-dependent growth inhibition by erythritol in Cutibacterium acnes."))

    def test_non_acne_lesions_keep_target_mapping(self) -> None:
        self.assertEqual(set(), self._acne_edges(
            "Zinc gluconate alleviated skin lesion severity in psoriasis patients.",
            target="skin lesion severity"))

    def test_evaluated_ingredients_include_claims_that_fail_the_gate(self) -> None:
        evaluated: set[str] = set()
        with tempfile.TemporaryDirectory() as temp_dir:
            batch = Path(temp_dir) / "batch=gate"
            batch.mkdir()
            row = {
                "ingredient_name": "Salicylic acid", "relation": "improves",
                "target": "acne", "effect_ids": "", "concern_ids": "",
                "eligibility_tier": "recommendation_only", "strength_label": "weak",
                "significance_label": "unclear", "attribution_label": "multi_active_combination",
                "claim_type": "efficacy", "source_sentence": "A combination improved acne.",
                "title": "", "study_context": "unknown",
                "all_detected_ingredients": "Salicylic acid", "pmid": "1", "row_weight": "0.1",
            }
            with (batch / "gold_claim_all.csv").open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(row))
                writer.writeheader()
                writer.writerow(row)
            with patch.object(build_gold_csvs, "CLAIM_BATCH_ROOT", Path(temp_dir)):
                edges = build_gold_csvs.load_affects_rows(
                    {6: "SEBUM_REGULATION"}, {"salicylic acid": "SALICYLIC ACID"},
                    claim_batch_id="gate", evaluated_ingredients=evaluated,
                )

        self.assertEqual([], edges)
        self.assertEqual({"SALICYLIC ACID"}, evaluated)


if __name__ == "__main__":
    unittest.main()


class ReviewEdgesTest(unittest.TestCase):
    def test_review_edges_require_human_papers_and_known_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            pd.DataFrame([
                {"ingredient_inci": "SALICYLIC ACID", "effect_code": "BLEMISH_CARE", "score": 2.159906,
                 "paper_count": 43, "human_paper_count": 34},
                {"ingredient_inci": "MELANIN", "effect_code": "PHOTOPROTECTIVE", "score": 0.06,
                 "paper_count": 1, "human_paper_count": 0},
                {"ingredient_inci": "NOT IN GRAPH", "effect_code": "HYDRATING", "score": 1.0,
                 "paper_count": 3, "human_paper_count": 2},
                {"ingredient_inci": "UREA", "effect_code": "UNKNOWN", "score": 1.0,
                 "paper_count": 3, "human_paper_count": 2},
            ]).to_csv(root / "review_edges.csv", index=False)
            pd.DataFrame({"ingredient_inci": ["SALICYLIC ACID", "MELANIN", "PROCOLLAGEN", "NOT IN GRAPH"]}).to_csv(
                root / "review_ingredients.csv", index=False)
            rows, reviewed = build_gold_csvs.load_review_affects_rows(
                root, {"SALICYLIC ACID", "MELANIN", "PROCOLLAGEN", "UREA"}, {"BLEMISH_CARE", "PHOTOPROTECTIVE", "HYDRATING"},
            )
        self.assertEqual(rows, [{
            ":START_ID(Ingredient)": "SALICYLIC ACID", ":END_ID(Effect)": "BLEMISH_CARE", "type": "improves",
            "evidence_type": "pubmed_evidence", "graph_score:float": 2.159906, "paper_count:int": 43,
        }])
        # 엣지가 없는 PROCOLLAGEN도 검수한 성분으로 남아 과거 논문 엣지를 되살리지 않는다.
        self.assertEqual(reviewed, {"SALICYLIC ACID", "MELANIN", "PROCOLLAGEN"})

    def test_reviewed_ingredients_drop_legacy_pubmed_edges(self) -> None:
        legacy = [
            {":START_ID(Ingredient)": "PROCOLLAGEN", ":END_ID(Effect)": "ANTI_AGING", "type": "improves",
             "evidence_type": "pubmed_evidence"},
            {":START_ID(Ingredient)": "PROCOLLAGEN", ":END_ID(Effect)": "ANTI_AGING", "type": "improves",
             "evidence_type": "reference_book"},
        ]
        kept = build_gold_csvs.retain_legacy_affects(
            legacy, {"PROCOLLAGEN"}, {"ANTI_AGING"}, set(), {"PROCOLLAGEN"},
        )
        self.assertEqual([r["evidence_type"] for r in kept], ["reference_book"])


class ReviewConcernEdgesTest(unittest.TestCase):
    def test_concern_edges_filter_unknown_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self.assertEqual(build_gold_csvs.load_review_concern_rows(root, {"UREA"}, {"ACNE"}), [])
            pd.DataFrame([
                {"ingredient_inci": "SALICYLIC ACID", "concern_code": "ACNE", "score": 2.1, "paper_count": 40,
                 "effects": "BLEMISH_CARE|COMEDOLYTIC"},
                {"ingredient_inci": "SALICYLIC ACID", "concern_code": "NOT_A_CONCERN", "score": 1.0, "paper_count": 1,
                 "effects": "X"},
                {"ingredient_inci": "UNKNOWN", "concern_code": "ACNE", "score": 1.0, "paper_count": 1, "effects": "X"},
            ]).to_csv(root / "review_concern_edges.csv", index=False)
            rows = build_gold_csvs.load_review_concern_rows(root, {"SALICYLIC ACID"}, {"ACNE"})
        self.assertEqual(rows, [{
            ":START_ID(Ingredient)": "SALICYLIC ACID", ":END_ID(Concern)": "ACNE", "evidence_type": "pubmed_review",
            "graph_score:float": 2.1, "paper_count:int": 40, "effects": "BLEMISH_CARE|COMEDOLYTIC",
        }])

    def test_seed_has_all_server_concerns(self) -> None:
        codes = {r["concern_code"] for r in build_gold_csvs.parse_concern_taxonomy()}
        with open(Path(__file__).resolve().parents[1] / "config" / "review" / "concern_conditions.csv",
                  encoding="utf-8") as handle:
            configured = {row["concern_code"] for row in csv.DictReader(handle)}
        self.assertEqual(configured - codes, set())
