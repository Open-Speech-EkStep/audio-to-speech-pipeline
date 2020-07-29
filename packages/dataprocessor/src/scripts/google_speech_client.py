import sys

sys.path.append('./transcription')

import pickle

from google.cloud import speech_v1
from google.cloud.speech_v1 import enums

from .gcs_operations import CloudStorageOperations


class GoogleSpeechClient(object):
    def __init__(self, language, sample_rate=16000, audio_channel_count=1):
        self.language = language
        self.sample_rate = sample_rate
        self.channels = audio_channel_count
        self.obj_gcs = CloudStorageOperations()

    def call_speech_to_text(self, input_file_path, save_response, dump_response_directory=None,
                            response_file_name=None):
        client = speech_v1.SpeechClient()

        config = {
            "language_code": self.language,
            "sample_rate_hertz": self.sample_rate,
            "encoding": enums.RecognitionConfig.AudioEncoding.LINEAR16,
            "audio_channel_count": self.channels,
            "enable_word_time_offsets": True,
            "enable_automatic_punctuation": False
        }

        print("Speech to Text API config to be used: {}".format(config))
        print("Source file path on GCS to be converted to text using API: {}".format(input_file_path))
        audio = {"uri": input_file_path}
        operation = client.long_running_recognize(config, audio)

        print(u"Waiting for Speech to Text API operation to complete...")
        response = operation.result()
        print("Speech to Text operation completed successfully")

        print("Flag value for dumping Speech to Text API response is: ", str(save_response))
        if save_response:
            if dump_response_directory is None:
                dump_response_directory = input_file_path + '/api-response-dump'

            print("Directory for dumping Speech to Text API response is: ", str(dump_response_directory))
            # Create API dump resposne directory if not exists
            self.obj_gcs.make_directories(dump_response_directory)

            with open(dump_response_directory + '/' + response_file_name + '.txt', "wb") as file:
                print('Dumping Speech to Text API response')
                pickle.dump(response, file)

        return response
