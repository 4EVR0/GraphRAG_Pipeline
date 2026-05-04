import unittest

from pipeline.gold.claim.evidence_scoring import (
    aggregate_canonical_rows,
    assert_canonical_score_order,
    build_canonical_claim_key,
    compute_eligibility_tier,
    compute_row_weight,
)


class EvidenceScoringTierTest(unittest.TestCase):
    def test_compute_eligibility_tier_covers_four_tiers(self) -> None:
        cases = [
            (
                "strict_graph",
                {
                    "strength_label": "strong",
                    "significance_label": "significant",
                    "attribution_label": "single_active",
                    "effect_ids": [101],
                    "concern_ids": [],
                    "sentence": "Niacinamide significantly improved skin barrier function.",
                    "detected_labels": ["Niacinamide"],
                },
            ),
            (
                "soft_graph",
                {
                    "strength_label": "moderate",
                    "significance_label": "significant",
                    "attribution_label": "single_formulation",
                    "effect_ids": [102],
                    "concern_ids": [],
                    "sentence": "A panthenol cream improved post-procedure dryness.",
                    "detected_labels": ["Panthenol"],
                },
            ),
            (
                "recommendation_only",
                {
                    "strength_label": "weak",
                    "significance_label": "unclear",
                    "attribution_label": "single_active",
                    "effect_ids": [103],
                    "concern_ids": [],
                    "sentence": "Centella may improve visible redness.",
                    "detected_labels": ["Centella asiatica"],
                },
            ),
            (
                "evidence_only",
                {
                    "strength_label": "strong",
                    "significance_label": "not_significant",
                    "attribution_label": "single_active",
                    "effect_ids": [104],
                    "concern_ids": [],
                    "sentence": "Retinol showed no significant improvement in wrinkles.",
                    "detected_labels": ["Retinol"],
                },
            ),
        ]

        for expected, kwargs in cases:
            with self.subTest(expected=expected):
                actual = compute_eligibility_tier(
                    kwargs["strength_label"],
                    kwargs["significance_label"],
                    kwargs["attribution_label"],
                    "efficacy",
                    kwargs["effect_ids"],
                    kwargs["concern_ids"],
                    sentence=kwargs["sentence"],
                    detected_labels=kwargs["detected_labels"],
                )
                self.assertEqual(expected, actual)

    def test_strict_graph_requires_single_detected_unit(self) -> None:
        tier = compute_eligibility_tier(
            "strong",
            "significant",
            "single_active",
            "efficacy",
            [201],
            [],
            sentence=(
                "A formulation containing niacinamide, panthenol, and ceramide "
                "significantly improved skin barrier function."
            ),
            detected_labels=["Niacinamide", "Panthenol", "Ceramide NP"],
        )

        self.assertEqual("recommendation_only", tier)


class EvidenceAggregationTest(unittest.TestCase):
    def test_aggregate_canonical_rows_preserves_graph_and_recommendation_scores(self) -> None:
        key = build_canonical_claim_key(
            "Niacinamide",
            "improves",
            "skin barrier",
            "barrier",
        )
        strict_weight = compute_row_weight(
            "strong",
            "significant",
            "single_active",
            "human_topical",
        )
        recommendation_weight = compute_row_weight(
            "weak",
            "unclear",
            "single_active",
            "human_topical",
        )

        rows = [
            {
                "canonical_claim_key": key,
                "pmid": "1",
                "row_weight": strict_weight,
                "eligibility_tier": "strict_graph",
                "attribution_label": "single_active",
                "ingredient_name": "Niacinamide",
                "relation": "improves",
                "target": "skin barrier",
                "target_category": "barrier",
                "effect_ids_list": [301],
                "concern_ids_list": [],
                "study_context": "human_topical",
            },
            {
                "canonical_claim_key": key,
                "pmid": "2",
                "row_weight": recommendation_weight,
                "eligibility_tier": "recommendation_only",
                "attribution_label": "single_active",
                "ingredient_name": "Niacinamide",
                "relation": "improves",
                "target": "skin barrier",
                "target_category": "barrier",
                "effect_ids_list": [301],
                "concern_ids_list": [],
                "study_context": "human_topical",
            },
        ]

        canonical_rows = aggregate_canonical_rows("batch-test", rows)

        self.assertEqual(1, len(canonical_rows))
        self.assertEqual("strict_graph", canonical_rows[0]["top_eligibility_tier"])
        self.assertTrue(canonical_rows[0]["is_graph_eligible"])
        self.assertGreater(
            canonical_rows[0]["recommendation_score_base"],
            canonical_rows[0]["graph_score"],
        )
        assert_canonical_score_order(canonical_rows)


if __name__ == "__main__":
    unittest.main()
