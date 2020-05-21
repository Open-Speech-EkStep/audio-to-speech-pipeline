import unittest
import sys

sys.path.append("../../src")

from scripts.snr import SNR


class TestSNR(unittest.TestCase):

    def test_compute_file_snr(self):
        obj = SNR()
        self.assertEqual(obj.compute_file_snr('test_resources/test.wav'), 19.939861)
