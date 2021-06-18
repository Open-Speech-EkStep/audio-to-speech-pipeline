import wave

import grpc
from ekstep_data_pipelines.common.utils import get_logger
from ekstep_data_pipelines.common.audio_commons.transcription_clients.stub.speech_recognition_open_api_pb2 import Language, RecognitionConfig, RecognitionAudio, \
    SpeechRecognitionRequest
from ekstep_data_pipelines.common.audio_commons.transcription_clients.stub.speech_recognition_open_api_pb2_grpc import SpeechRecognizerStub


LOGGER = get_logger("EkstepTranscriptionClient")


class EkstepTranscriptionClient(object):
    @staticmethod
    def get_instance(config_dict):
        ekstep_config_dict = config_dict.get("common", {}).get(
            "ekstep_transcription_client", {}
        )
        return EkstepTranscriptionClient(**ekstep_config_dict)

    def __init__(self, **kwargs):
        self.server_host = kwargs.get("server_host")
        self.port = kwargs.get("port")
        self.language = kwargs.get("language", "hi")
        self.channel = grpc.insecure_channel(self.server_host + ':' + self.port)
        self.stub = SpeechRecognizerStub(self.channel)
        self.language_config = Language(value=self.language)
        self.speech_config = RecognitionConfig(language=self.language_config, enableAutomaticPunctuation=True)
        self.audio_config = None

    def read_audio(self, audio_file_path):
        with wave.open(audio_file_path, 'rb') as f:
            return f.readframes(f.getnframes())

    def generate_transcription(self, language, source_file_path):
        try:
            result = self.speech_to_text(source_file_path)
        except RuntimeError as error:
            print(str(error))
            raise EkstepTranscriptionClient()
        return result.transcript

    def speech_to_text(self, audio_file_path):
        audio_input = RecognitionAudio(audioContent=self.read_audio(audio_file_path))

        LOGGER.info("Calling ekstep stt API for file: %s", audio_file_path)
        LOGGER.info("Recognizing first result...")
        request = SpeechRecognitionRequest(audio=audio_input, config=self.speech_config)
        try:
            result = self.stub.recognize(request)
            return result
        except grpc.RpcError as e:
            raise RuntimeError(e)


if __name__ == '__main__':
    # import yaml
    #
    # with open('real_test_config.yaml', "r") as file:
    #     parent_config_dict = yaml.load(file)
    #     initialization_dict = parent_config_dict.get("config")
    #
    #
    # ekstep_transcription_client = EkstepTranscriptionClient.get_instance(
    #     initialization_dict
    # )
    #
    # res = ekstep_transcription_client.generate_transcription('hi', 'download.wav')
    # print(res)
    pass
