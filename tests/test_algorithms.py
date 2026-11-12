"""Synthetic algorithm contracts complementary to the focused algorithm tests."""

import unittest

import numpy as np

from src.algorithms.cure import _cure
from src.algorithms.son import (
    _apriori,
    brute_force_frequent_itemsets,
    validate_no_false_negatives,
)


class SyntheticAlgorithmContractTests(unittest.TestCase):
    def test_son_synthetic_graph_transactions(self) -> None:
        transactions = [(1, 2, 3), (1, 2), (1, 3), (2, 3)]
        candidates = _apriori(transactions, minimum_count=2, max_itemset_size=2)
        expected = brute_force_frequent_itemsets(transactions, 2, 2)
        self.assertTrue(validate_no_false_negatives(transactions, candidates, 2, 2))
        self.assertEqual(set(candidates), expected)

    def test_cure_synthetic_graph_feature_blobs(self) -> None:
        points = np.asarray(
            [[0.0, 0.0], [0.1, 0.0], [0.0, 0.1], [10.0, 10.0], [10.1, 10.0], [10.0, 10.1]]
        )
        labels, representatives = _cure(
            list(range(len(points))),
            points,
            cluster_count=2,
            representative_count=2,
            shrink_factor=0.2,
        )
        self.assertEqual(len(set(labels.tolist())), 2)
        self.assertEqual(len(representatives), 2)
        self.assertEqual(labels[0], labels[1])
        self.assertEqual(labels[3], labels[4])

    def test_all_algorithm_modules_have_configured_entrypoints(self) -> None:
        from src.algorithms.cure import run_configured_cure
        from src.algorithms.hits import run_configured_hits
        from src.algorithms.label_propagation import run_configured_label_propagation
        from src.algorithms.pagerank import run_configured_pagerank
        from src.algorithms.son import run_configured_son
        from src.algorithms.triangle_count import run_configured_triangle_count

        for entrypoint in (
            run_configured_pagerank,
            run_configured_hits,
            run_configured_label_propagation,
            run_configured_triangle_count,
            run_configured_son,
            run_configured_cure,
        ):
            self.assertTrue(callable(entrypoint))


if __name__ == "__main__":
    unittest.main()
