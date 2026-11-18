#!/usr/bin/env python3
"""Check weighted summaries and trimming, without interpreting synthetic load as hardware data."""
import importlib.util
from pathlib import Path
import unittest

path = Path(__file__).resolve().parents[1] / "bench/peer-e2e/fence_stats.py"
spec = importlib.util.spec_from_file_location("fence_stats", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def line(stamp, samples, total, hist, peak=4, stalls=0):
    fields = {"mode": "write-imm", "worker": 0, "eu": 0, "cookie": 1, "lane": 0,
              "generation": 1, "sample": "post_commit", "unix_ns": stamp,
              "window_ms": 1000, "capacity": 32, "current": 0, "samples": samples,
              "depth_sum": total, "max": peak, "full_stalls": stalls}
    fields.update({name: value for (name, _), value in zip(module.BUCKETS, hist)})
    return "WRITE_IMM_FENCE_STATS " + " ".join(f"{key}={value}" for key, value in fields.items())


class FenceStatsTest(unittest.TestCase):
    def test_weighted_mean_duplicate_and_histogram(self):
        first = line(2000000000, 1, 1, [1, 0, 0, 0, 0], peak=1)
        second = line(3000000000, 9, 36, [0, 9, 0, 0, 0], stalls=2)
        row, = module.summarize([first, second, first])
        self.assertEqual(row["windows"], 2)
        self.assertEqual(row["avg"], 3.7)
        self.assertEqual(row["p95_bucket"], [2, 4])
        self.assertEqual(row["full_stalls"], 2)

    def test_whole_windows_and_idle(self):
        rows = [line(2000000000, 1, 1, [1, 0, 0, 0, 0]),
                line(3000000000, 0, 0, [0, 0, 0, 0, 0])]
        row, = module.summarize(rows, 2000000000, 3000000000)
        self.assertEqual(row["windows"], 1)
        self.assertIsNone(row["avg"])
        self.assertIsNone(row["p95_bucket"])

    def test_saturated_histogram_does_not_claim_percentiles(self):
        row, = module.summarize([line(2000000000, 6, 6, [5, 0, 0, 0, 0])])
        self.assertFalse(row["histogram_complete"])
        self.assertIsNone(row["p95_bucket"])


if __name__ == "__main__":
    unittest.main()
