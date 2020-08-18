import sys
import os

sys.path.append('./transcription')
sys.path.insert(0, '..')
sys.path.insert(0, '../..')
sys.path.insert(0, '../../...')


import pickle

from google.cloud import speech_v1
from google.cloud.speech_v1 import enums

from packages.ekstep_data_pipelines.common.utils import get_logger

LOGGER = get_logger('GoogleTranscriptionClient')


class GoogleTranscriptionClient(object):

    @staticmethod
    def get_instance(config_dict):
        google_config_dict = config_dict.get('common', {}).get('google_transcription_client')
        return GoogleTranscriptionClient(**google_config_dict)

    def __init__(self, **config_dict):
        self.language = config_dict.get('language', 'hi-IN')
        self.sample_rate = config_dict.get('sample_rate', 16000)
        self.channels = config_dict.get('audio_channel_count', 1)
        self._client = None

    def make_directories(self, path):
        if not os.path.exists(path):
            LOGGER(f"Directory {path} not does already exist")
            os.makedirs(path)
            LOGGER.info(f"Directory {path} created successfully")

    @property
    def config(self):
        return {
            "language_code": self.language,
            "sample_rate_hertz": self.sample_rate,
            "encoding": enums.RecognitionConfig.AudioEncoding.LINEAR16,
            "audio_channel_count": self.channels,
            "enable_word_time_offsets": True,
            "enable_automatic_punctuation": False
        }

    @property
    def client(self):
        if not self._client:
            self._client = speech_v1.SpeechClient()

        return self._client

    def generate_transcription(self, language, source_file_path):
        content = self.call_speech_to_text(source_file_path, language)
        transcriptions = list(map(lambda c: c.alternatives[0].transcript, content.results))
        return ' '.join(transcriptions)

    def call_speech_to_text(self, input_file_path):

        LOGGER.info(f'Queuing operation on GCP for {input_file_path}')
        operation = self.client.long_running_recognize(self.config, {"uri": input_file_path})

        LOGGER.info(f'Waiting for {operation} to complete on GCP for {input_file_path}')
        response = operation.result()

        return response
