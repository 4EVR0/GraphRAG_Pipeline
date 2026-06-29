import unittest

from pipeline.claim.services.claim_extractor import ClaimExtractor
from pipeline.claim.services.claim_filter import is_claim_candidate_sentence


class SebumContextFilterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.extractor = ClaimExtractor()

    def test_niacinamide_sebum_contexts_reach_claim_extraction(self) -> None:
        sentences = [
            "Niacinamide significantly reduced sebum production.",
            "Niacinamide significantly reduced oil production.",
            "Niacinamide improved oily skin.",
            "Niacinamide significantly reduced facial pore size.",
            "Niacinamide improved seborrhea.",
        ]

        for sentence in sentences:
            with self.subTest(sentence=sentence):
                self.assertTrue(is_claim_candidate_sentence(sentence))
                self.assertEqual(
                    ["Niacinamide"],
                    self.extractor.extract_ingredient_names(sentence),
                )

    def test_other_sebum_ingredients_reach_claim_extraction(self) -> None:
        cases = [
            (
                "Salicylic acid significantly reduced sebum production.",
                "Salicylic acid",
            ),
            ("Zinc PCA significantly improved oily skin.", "Zinc PCA"),
        ]

        for sentence, expected_ingredient in cases:
            with self.subTest(sentence=sentence):
                self.assertTrue(is_claim_candidate_sentence(sentence))
                self.assertEqual(
                    [expected_ingredient],
                    self.extractor.extract_ingredient_names(sentence),
                )

    def test_oil_requires_a_complete_word(self) -> None:
        sentence = "Niacinamide significantly improved spoiled samples."

        self.assertFalse(is_claim_candidate_sentence(sentence))
        self.assertEqual([], self.extractor.extract_ingredient_names(sentence))

    def test_metabolic_niacinamide_context_remains_blocked(self) -> None:
        sentence = (
            "Niacinamide significantly increased NAD metabolism "
            "and reduced sebum production."
        )

        self.assertTrue(is_claim_candidate_sentence(sentence))
        self.assertEqual([], self.extractor.extract_ingredient_names(sentence))


class EffectTaxonomyMappingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.extractor = ClaimExtractor()
        cls.effect_rows = [
            {
                "effect_id": 1,
                "effect_code": "KERATOLYTIC",
                "effect_name_en": "Keratolytic",
            }
        ]

    def test_keratolytic_language_maps_to_keratolytic_effect(self) -> None:
        targets = [
            "keratolysis",
            "gentle scab removal",
            "superficial peels",
            "skin texture and roughness",
        ]

        for target in targets:
            with self.subTest(target=target):
                self.assertEqual(
                    [1],
                    self.extractor.extract_effect_ids(
                        target,
                        "improves",
                        self.effect_rows,
                    ),
                )

    def test_taxonomy_mapping_uses_source_sentence_context(self) -> None:
        maps = self.extractor.infer_taxonomy_maps(
            {
                "target": "hydration",
                "source_sentence": "Urea promoted hydration and keratolysis.",
                "relation": "improves",
            },
            self.effect_rows,
            [],
        )

        self.assertEqual([1], maps["effect_ids"])


if __name__ == "__main__":
    unittest.main()
