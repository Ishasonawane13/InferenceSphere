import unittest

from src.algorithms.cure import _select_representatives, cure_cluster


class CURETests(unittest.TestCase):
    def test_two_well_separated_blobs_form_two_clusters(self) -> None:
        points = [
            (-2.0, -2.0),
            (-2.1, -1.9),
            (-1.9, -2.1),
            (2.0, 2.0),
            (2.1, 1.9),
            (1.9, 2.1),
        ]

        labels = cure_cluster(points, k=2, c=2, alpha=0.5)

        self.assertEqual(len(set(labels)), 2)
        self.assertEqual(len(set(labels[:3])), 1)
        self.assertEqual(len(set(labels[3:])), 1)
        self.assertNotEqual(labels[0], labels[3])

    def test_representatives_are_well_scattered_and_shrunk(self) -> None:
        points = [(0.0,), (2.0,), (10.0,), (12.0,)]

        representatives = _select_representatives(
            points,
            count=2,
            alpha=0.5,
        )

        self.assertEqual(representatives, [(3.0,), (9.0,)])


if __name__ == "__main__":
    unittest.main()
