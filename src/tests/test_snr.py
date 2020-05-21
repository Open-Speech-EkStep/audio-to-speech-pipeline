import unittest
import sys

sys.path.append("../../src")

from scripts.snr import SNR


class TestSNR(unittest.TestCase):

    def test_compute_file_snr(self):
        obj = SNR()
        self.assertEqual(obj.compute_file_snr('test_resources/test.wav'), 19.939861)

    def test_fit_return_type(self):
        obj = SNR()
        file_snr = obj.fit('test_resources/')
        self.assertEqual(type(file_snr), type({}))

    def test_fit_return_value(self):
        obj = SNR()
        file_snr = obj.fit('test_resources/')
        self.assertEqual(file_snr['test_resources/test.wav'], 19.939861)