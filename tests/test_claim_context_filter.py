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


if __name__ == "__main__":
    unittest.main()
