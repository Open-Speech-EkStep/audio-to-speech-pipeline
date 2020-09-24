import unittest
from unittest import mock
import sys
import os
import shutil

from unittest.mock import MagicMock, patch, Mock


from azure.cognitiveservices.speech import speech
from common.audio_commons.transcription_clients.google_transcription_client import GoogleTranscriptionClient

sys.path.insert(0, '..')


class TestGoogleTranscriptionClient(unittest.TestCase):

    def setUp(self):
        super(TestGoogleTranscriptionClient, self).setUp()

        config = {"sample_rate": 1234, "language": "Hi-IN"}

        self.google_client = GoogleTranscriptionClient(**config)

    @mock.patch("pickle.dump")
    def test_call_speech_to_text(self, mock_dump):

        mock_client = Mock()

        self.google_client._client = mock_client

        mock_new_result = Mock()

        mock_client.long_running_recognize.return_value = mock_new_result

        mock_new_result.result.return_value = mock_new_result

        mock_new_result.results = [mock_new_result]

        mock_new_result.alternatives = [mock_new_result]

        mock_new_result.transcript = " कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है।"

        actual_result = self.google_client.generate_transcription("test_language","input_file_path")

        self.assertEqual(mock_client.long_running_recognize.call_count,1)

        self.assertEqual(actual_result," कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है।")
