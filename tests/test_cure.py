import unittest

import numpy as np

from src.algorithms.cure import _cure, _representatives


class CURETests(unittest.TestCase):
    def test_representatives_are_shrunk_toward_centroid(self) -> None:
        points = np.asarray([[0.0, 0.0], [10.0, 0.0], [0.0, 10.0]])
        representatives = _representatives(points, 2, 0.2)
        centroid = points.mean(axis=0)
        self.assertEqual(len(representatives), 2)
        self.assertTrue(
            np.all(
                np.linalg.norm(representatives - centroid, axis=1)
                <= np.linalg.norm(points - centroid, axis=1).max()
            )
        )

    def test_two_separated_blobs_form_two_clusters(self) -> None:
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
        self.assertNotEqual(labels[0], labels[3])


if __name__ == "__main__":
    unittest.main()
