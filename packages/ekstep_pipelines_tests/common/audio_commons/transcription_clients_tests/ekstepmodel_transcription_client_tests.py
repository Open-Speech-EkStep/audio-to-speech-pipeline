import unittest
from unittest import mock
from unittest.mock import Mock

from ekstep_data_pipelines.common.audio_commons.transcription_clients.azure_transcription_client import (
    EkstepTranscriptionClient,
)


class TestEkstepTranscriptionClient(unittest.TestCase):
    def setUp(self):
        super(TestEkstepTranscriptionClient, self).setUp()

        config = {"server_host": '127.0.0.1', "port": '50051', "language": "hi"}

        self.ekstep_client = EkstepTranscriptionClient(**config)

    @mock.patch("pickle.dump")
    def test_call_speech_to_text_ekstep(self, mock_dump):
        mock_client = Mock()

        self.ekstep_client.client = mock_client

        mock_new_result = Mock()

        mock_client.recognize.return_value = mock_new_result

        mock_new_result.transcript = (
            " कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है।"
        )

        actual_result = self.ekstep_client.generate_transcription(
            "test_language", "input_file_path"
        )

        self.assertEqual(mock_client.recognize.call_count, 1)

        self.assertEqual(
            actual_result, " कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है।"
        )
