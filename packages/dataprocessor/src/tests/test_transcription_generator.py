import os
import pickle
import shutil
import unittest
from unittest import mock

from src.scripts.transcription_generator import save_transcriptions, create_transcriptions, create_azure_transcription, \
    create_google_transcription


class TestTrancriptionGenerator(unittest.TestCase):
    @mock.patch("src.scripts.google_speech_client.GoogleSpeechClient")
    def test_create_transcriptions(self, mock_client):
        output_file_dir = "./src/tests/test_resources/output/transcriptions/1"
        os.makedirs(output_file_dir)
        transcriptions = create_transcriptions(mock_client, "wav_file_path", output_file_dir, "api_response_file")
        if os.path.exists(output_file_dir):
            shutil.rmtree(output_file_dir)
        self.assertEquals(transcriptions, [''])


    def test_save_transcriptions(self):
        transcriptions = ['', 'मेरे प्यारे देशवासियों', 'नमस्कार', '', 'कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है', 'जब मैंने पिछली बार आपसे']
        output_file_dir = './src/tests/test_resources/output/saved_transcriptions'
        os.makedirs(output_file_dir)
        save_transcriptions(output_file_dir, transcriptions, 'chunk')
        files = os.listdir(output_file_dir)
        if os.path.exists(output_file_dir):
            shutil.rmtree(output_file_dir)
        self.assertEqual(len(files), 6)

    @mock.patch("src.scripts.azure_speech_client.AzureSpeechClient")
    def test_create_azure_transcription_wo_punctuation(self, mock_client):
        mocked_result = mock.Mock()
        mocked_result.text = 'कोरोना के प्रभाव से: हमारी मन की बात भी अछूती नहीं रही है।'
        mock_client.speech_to_text.return_value = mocked_result
        output_file_dir = "./src/tests/test_resources/output/transcriptions/2"
        wave_file_path = output_file_dir + '/chunk.wav'
        os.makedirs(output_file_dir)
        transcription = create_azure_transcription(mock_client, 'hi-IN', wave_file_path)
        files = os.listdir(output_file_dir)
        if os.path.exists(output_file_dir):
            shutil.rmtree(output_file_dir)
        self.assertEquals(len(files), 1)
        self.assertEquals(transcription, 'कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है')

    @mock.patch("src.scripts.azure_speech_client.AzureSpeechClient")
    def test_create_azure_transcription_with_punctuation(self, mock_client):
        mocked_result = mock.Mock()
        mocked_result.text = 'कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है।'
        mock_client.speech_to_text.return_value = mocked_result
        output_file_dir = "./src/tests/test_resources/output/transcriptions/3"
        wave_file_path = output_file_dir + '/chunk.wav'
        os.makedirs(output_file_dir)
        transcription = create_azure_transcription(mock_client, 'hi-IN', wave_file_path, True)
        files = os.listdir(output_file_dir)
        if os.path.exists(output_file_dir):
            shutil.rmtree(output_file_dir)
        self.assertEquals(len(files), 1)
        self.assertEquals(transcription, 'कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है।')

    @mock.patch("src.scripts.google_speech_client.GoogleSpeechClient")
    def test_create_google_transcription(self, mock_client):
        path_srt_file = './src/tests/test_resources/input/merged.txt'
        with open(path_srt_file, 'rb') as file:
            content = pickle.load(file)
        mock_client.call_speech_to_text.return_value = content
        output_file_dir = "./src/tests/test_resources/output/transcriptions/4"
        wave_file_path = output_file_dir + '/chunk.wav'
        os.makedirs(output_file_dir)
        transcription = create_google_transcription(mock_client, wave_file_path)
        files = os.listdir(output_file_dir)
        if os.path.exists(output_file_dir):
            shutil.rmtree(output_file_dir)
        self.assertEquals(len(files), 1)
        self.assertEquals(transcription, 'हॉपिपोला मेरे प्यारे देशवासियों नमस्कार  हॉपिपोला कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है')
if __name__ == '__main__':
    unittest.main()
