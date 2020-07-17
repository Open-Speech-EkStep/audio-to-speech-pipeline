import unittest
from unittest import mock
import os
import shutil
from src.scripts.google_speech_client import GoogleSpeechClient


class TestGoogleSpeechClient(unittest.TestCase):
    @mock.patch("google.cloud.speech_v1.SpeechClient")
    @mock.patch("pickle.dump")
    def test_call_speech_to_text(self, mock_client, mock_dump):
        ob = GoogleSpeechClient("hi")
        c = ob.call_speech_to_text("input_file_path", "save_response", "dump_response_directory", "response_file_name")
        self.assertEqual(True, os.path.isfile("dump_response_directory/response_file_name.txt"))
        shutil.rmtree("dump_response_directory")


if __name__ == '__main__':
    unittest.main()
