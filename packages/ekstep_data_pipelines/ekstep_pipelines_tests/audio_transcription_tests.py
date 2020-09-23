import sys
import unittest
from audio_processing import constants
from unittest.mock import Mock


from audio_transcription.audio_transcription import AudioTranscription
sys.path.insert(0, '..')


class AudioTranscriptionTests(unittest.TestCase):

    def setUp(self):
        self.postgres_client = Mock()
        self.gcp_instance = Mock()
        self.audio_commons = {"snr_util": Mock(), "chunking_conversion": Mock()}
        self.catalogue_dao = Mock()
        self.audio_processer = AudioTranscription(self.postgres_client, self.gcp_instance,self.audio_commons,self.catalogue_dao)

    def test__dummy_test(self):
        self.assertEqual(1,1)

    