import unittest
from unittest import mock
import sys

from azure.cognitiveservices.speech import speech
from ekstep_data_pipelines.common.audio_commons.transcription_clients.azure_transcription_client import AzureTranscriptionClient
from ekstep_data_pipelines.common.audio_commons.transcription_clients.transcription_client_errors import AzureTranscriptionClientError



class TestAzureTranscriptionClient(unittest.TestCase):

    def setUp(self):
        super(TestAzureTranscriptionClient, self).setUp()

        config = {"speech_key":"dummy_key","service_region":"centralindia"}

        self.azure_client = AzureTranscriptionClient(**config)


    @mock.patch("azure.cognitiveservices.speech.SpeechRecognizer")
    def test__speech_to_text_success(self, mock_speechrecongnizer):

        result = mock.Mock()
        result.text = 'कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है।'
        result.reason = speech.ResultReason.RecognizedSpeech
        mock_speechrecongnizer.return_value.recognize_once.return_value = result

        audio_file_path = 'chunk-2.wav'
        actual_value = self.azure_client.generate_transcription( 'hi-IN',audio_file_path)
        self.assertEqual('कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है।', actual_value)

    @mock.patch("azure.cognitiveservices.speech.SpeechRecognizer")
    def test__speech_to_text_no_match(self, mock_speechrecongnizer):
        result = mock.Mock()
        result.text = None
        result.reason = speech.ResultReason.NoMatch
        result.no_match_details = "test_api_error"
        mock_speechrecongnizer.return_value.recognize_once.return_value = result

        audio_file_path = 'chunk-2.wav'
        self.assertRaises(AzureTranscriptionClientError, self.azure_client.generate_transcription, 'hi-IN',audio_file_path)

    @mock.patch("azure.cognitiveservices.speech.SpeechRecognizer")
    def test__speech_to_text_cancelled(self, mock_speechrecongnizer):
        result = mock.Mock()
        result.text = None
        result.reason = speech.ResultReason.Canceled
        mock_speechrecongnizer.return_value.recognize_once.return_value = result

        audio_file_path = 'chunk-2.wav'
        self.assertRaises(AzureTranscriptionClientError, self.azure_client.generate_transcription, 'hi-IN',audio_file_path)
