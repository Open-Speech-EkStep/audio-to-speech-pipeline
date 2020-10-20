import sys
import unittest

from audio_processing.generate_hash import get_hash_code_of_audio_file

sys.path.insert(0, '..')


class GenerateHashTests(unittest.TestCase):

    def test_get_hash_code_of_audio_file(self):
        actual_output = get_hash_code_of_audio_file('./ekstep_pipelines_tests/resources/chunk.wav')

        self.assertEqual(actual_output,"a5449c38212f1a3bc1b6bcb2bd2cf678")

