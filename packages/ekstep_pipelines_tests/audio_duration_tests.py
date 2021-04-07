import unittest

from ekstep_data_pipelines.audio_processing.audio_duration import (
    calculate_duration,
    calculate_duration_librosa,
)


class AudioDurationTests(unittest.TestCase):
    def test_librosa(self):
        duration = calculate_duration_librosa(
            "ekstep_pipelines_tests/resources/chunk.wav"
        )
        print(duration)
        self.assertEquals(6.690022675736961, duration)

    def test_sox(self):
        duration_sox = calculate_duration("ekstep_pipelines_tests/resources/chunk.wav")
        print(duration_sox)
        self.assertEqual(6.69, duration_sox)
