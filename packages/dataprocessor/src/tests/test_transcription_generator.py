import unittest
from unittest import mock
from src.scripts.transcription_generator import create_transcription


class TestTrancriptionGenerator(unittest.TestCase):
    @mock.patch("src.scripts.google_speech_client.GoogleSpeechClient")
    def test_create_transcription(self, mock_client):
        response = create_transcription(mock_client, "wav_file_path", "output_path", "api_response_file")
        self.assertEquals(response, ['output_path/chunk-0.wav'])


if __name__ == '__main__':
    unittest.main()
