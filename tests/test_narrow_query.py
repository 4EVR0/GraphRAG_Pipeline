import unittest
from pathlib import Path

from pipeline.metadata.services.narrow_query import (
    NARROW_FILTER,
    EffectTerms,
    build_pair_query,
    drop_self_matching_terms,
    ingredient_terms,
    load_effect_terms,
    load_ingredient_rules,
    load_pairs,
    narrowed_query,
    plan_pairs,
)

CONFIG = Path(__file__).resolve().parents[1] / "config" / "pubmed_narrow"

EFFECTS = {
    "COMEDOLYTIC": EffectTerms(("acne", "comedone"), skin_specific=True),
    "HYDRATING": EffectTerms(("hydration", "dry skin"), skin_specific=False),
    "DEPIGMENTING": EffectTerms(("hyperpigmentation", "melanin"), skin_specific=True),
    "BARRIER_REPAIR": EffectTerms(("skin barrier",), skin_specific=True),
}


class IngredientTermsTest(unittest.TestCase):
    def test_ambiguous_short_abbreviation_is_dropped(self) -> None:
        synonyms = {"SALICYLIC ACID": ["Salicylic Acid", "SALICYLIC ACID", "BHA", "beta hydroxy acid"]}
        self.assertEqual(
            ingredient_terms("SALICYLIC ACID", synonyms),
            ["SALICYLIC ACID", "beta hydroxy acid"],
        )

    def test_inci_name_is_kept_even_if_short(self) -> None:
        self.assertEqual(ingredient_terms("DNA", {"DNA": ["DNA"]}), ["DNA"])

    def test_quotes_are_removed(self) -> None:
        self.assertEqual(ingredient_terms('A "B" OIL', {}), ["A B OIL"])


class SelfMatchTest(unittest.TestCase):
    def test_effect_term_equal_to_ingredient_is_dropped(self) -> None:
        self.assertEqual(
            drop_self_matching_terms(["MELANIN"], ("hyperpigmentation", "melanin", "tyrosinase")),
            ["hyperpigmentation", "tyrosinase"],
        )

    def test_word_contained_in_ingredient_is_dropped(self) -> None:
        self.assertEqual(
            drop_self_matching_terms(["HYDROLYZED COLLAGEN"], ("wrinkle", "collagen")),
            ["wrinkle"],
        )

    def test_partial_word_is_not_self_match(self) -> None:
        self.assertEqual(drop_self_matching_terms(["PROCOLLAGEN"], ("collagen",)), ["collagen"])


class QueryTest(unittest.TestCase):
    def test_skin_specific_effect_has_no_skin_context(self) -> None:
        query = build_pair_query(["SALICYLIC ACID"], ["acne"], skin_specific=True)
        self.assertEqual(
            query,
            '("SALICYLIC ACID"[tiab]) AND ("acne"[tiab]) AND hasabstract AND english[lang]',
        )

    def test_general_effect_gets_skin_context(self) -> None:
        query = build_pair_query(["UREA"], ["hydration"], skin_specific=False)
        self.assertIn('"skin"[tiab]', query)
        self.assertIn('"dermatolog*"[tiab]', query)
        self.assertNotIn('"cream"[tiab]', query)

    def test_require_topical_adds_mandatory_clause(self) -> None:
        query = build_pair_query(["GLUCOSE"], ["skin barrier"], skin_specific=True, require_topical=True)
        self.assertTrue(query.endswith('"skincare"[tiab])'))
        self.assertIn(' AND ("topical"[tiab] OR ', query)
        self.assertNotIn('"serum"', query)

    def test_narrowed_query_appends_human_trial_review_filter(self) -> None:
        self.assertEqual(narrowed_query("Q"), f"Q AND {NARROW_FILTER}")
        self.assertIn("humans[mh]", NARROW_FILTER)
        self.assertIn("randomized controlled trial[pt]", NARROW_FILTER)
        self.assertIn("meta-analysis[pt]", NARROW_FILTER)


class PlanPairsTest(unittest.TestCase):
    def test_rules_and_skip_reasons(self) -> None:
        pairs = [
            {"inci_name": "ALCOHOL", "effect_code": "HYDRATING"},
            {"inci_name": "MELANIN", "effect_code": "DEPIGMENTING"},
            {"inci_name": "GLUCOSE", "effect_code": "BARRIER_REPAIR"},
            {"inci_name": "UREA", "effect_code": "UNKNOWN"},
            {"inci_name": "SALICYLIC ACID", "effect_code": "COMEDOLYTIC"},
        ]
        rules = {"ALCOHOL": "exclude", "MELANIN": "require_topical", "GLUCOSE": "require_topical"}
        plans = {p.inci_name: p for p in plan_pairs(pairs, {}, EFFECTS, rules)}

        self.assertEqual(plans["ALCOHOL"].skip_reason, "excluded_ingredient")
        self.assertIsNone(plans["ALCOHOL"].query)
        self.assertEqual(plans["MELANIN"].effect_terms, ("hyperpigmentation",))
        self.assertNotIn('"melanin"[tiab]', plans["MELANIN"].query)
        self.assertIn('"topical"[tiab]', plans["GLUCOSE"].query)
        self.assertEqual(plans["UREA"].skip_reason, "no_effect_terms")
        self.assertNotIn('"topical"[tiab]', plans["SALICYLIC ACID"].query)

    def test_all_effect_terms_self_matching_is_skipped(self) -> None:
        effects = {"X": EffectTerms(("melanin",), skin_specific=True)}
        (plan,) = plan_pairs([{"inci_name": "MELANIN", "effect_code": "X"}], {}, effects)
        self.assertEqual(plan.skip_reason, "self_match")


class VersionedConfigTest(unittest.TestCase):
    def test_every_pair_effect_has_terms(self) -> None:
        effects = load_effect_terms(CONFIG / "effect_terms.csv")
        pairs = load_pairs(CONFIG / "tier1_pairs.csv")
        self.assertEqual(len(pairs), 769)
        self.assertEqual({p["effect_code"] for p in pairs} - set(effects), set())
        self.assertNotIn("BLEMISH_CARE", {p["effect_code"] for p in pairs})

    def test_rules_load(self) -> None:
        rules = load_ingredient_rules(CONFIG / "ingredient_rules.csv")
        self.assertEqual(rules["ALCOHOL"], "exclude")
        self.assertEqual(rules["MELANIN"], "require_topical")


if __name__ == "__main__":
    unittest.main()
