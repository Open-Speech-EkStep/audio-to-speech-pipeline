import unittest
from unittest import mock

from azure.cognitiveservices.speech import speech
from packages.dataprocessor.src.scripts.azure_speech_client import AzureSpeechClient


class TestAzureSpeechClient(unittest.TestCase):

    # def test_real_call(self):
    #     azureClient = AzureSpeechClient('e3160461990a42e2abe91405b7321de4', 'centralindia')
    #     audio_file_path = '/Users/rajats/projects/ekstep/audio-to-speech-pipeline/packages/dataprocessor/src/tests/test_resources/input/chunks/chunk-2.wav'
    #     result = azureClient.speech_to_text(audio_file_path, 'hi-IN')
    #     self.assertEquals('कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है।', result.text)

    @mock.patch("azure.cognitiveservices.speech.SpeechRecognizer")
    def test_speech_to_text_success(self, mock_speechrecongnizer):
        result = mock.Mock()
        result.text = 'कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है।'
        result.reason = speech.ResultReason.RecognizedSpeech
        mock_speechrecongnizer.return_value.recognize_once.return_value = result
        azure_client = AzureSpeechClient('dummy_key', 'centralindia')
        audio_file_path = 'chunk-2.wav'
        result = azure_client.speech_to_text(audio_file_path, 'hi-IN')
        self.assertEqual('कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है।', result.text)

    @mock.patch("azure.cognitiveservices.speech.SpeechRecognizer")
    def test_speech_to_text_no_match(self, mock_speechrecongnizer):
        result = mock.Mock()
        result.text = None
        result.reason = speech.ResultReason.NoMatch
        mock_speechrecongnizer.return_value.recognize_once.return_value = result
        azure_client = AzureSpeechClient('dummy_key', 'centralindia')
        audio_file_path = 'chunk-2.wav'
        result = azure_client.speech_to_text(audio_file_path, 'hi-IN')
        self.assertEqual(None, result)

    @mock.patch("azure.cognitiveservices.speech.SpeechRecognizer")
    def test_speech_to_text_cancelled(self, mock_speechrecongnizer):
        result = mock.Mock()
        result.text = None
        result.reason = speech.ResultReason.Canceled
        mock_speechrecongnizer.return_value.recognize_once.return_value = result
        azure_client = AzureSpeechClient('dummy_key', 'centralindia')
        audio_file_path = 'chunk-2.wav'
        result = azure_client.speech_to_text(audio_file_path, 'hi-IN')
        self.assertEqual(None, result)
