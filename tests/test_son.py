import unittest

from src.algorithms.son import (
    _apriori,
    brute_force_frequent_itemsets,
    validate_no_false_negatives,
)


class SONTests(unittest.TestCase):
    def setUp(self) -> None:
        self.transactions = [
            (1, 2, 3),
            (1, 2),
            (1, 3),
            (2, 3),
        ]

    def test_manual_frequent_itemsets(self) -> None:
        expected = {(1,), (2,), (3,), (1, 2), (1, 3), (2, 3)}
        actual = brute_force_frequent_itemsets(self.transactions, 2, 3)
        self.assertEqual(actual, expected)

    def test_local_apriori_has_no_false_negatives(self) -> None:
        actual = _apriori(self.transactions, 2, 3)
        self.assertTrue(
            validate_no_false_negatives(self.transactions, actual, 2, 3)
        )

    def test_four_user_example_support_counts(self) -> None:
        transactions = [(1, 2), (1, 2, 3), (1, 3), (2, 3)]
        result = brute_force_frequent_itemsets(transactions, 3, 2)
        self.assertEqual(result, {(1,), (2,), (3,)})


if __name__ == "__main__":
    unittest.main()
