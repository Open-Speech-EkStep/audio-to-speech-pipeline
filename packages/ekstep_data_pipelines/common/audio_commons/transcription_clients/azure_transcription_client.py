from azure.cognitiveservices import speech
from ekstep_data_pipelines.common.audio_commons.transcription_clients.transcription_client_errors \
    import (
    AzureTranscriptionClientError, )
from ekstep_data_pipelines.common.utils import get_logger

LOGGER = get_logger("AzureTranscriptionClient")


class AzureTranscriptionClient(object):
    @staticmethod
    def get_instance(config_dict):
        azure_config_dict = config_dict.get("common", {}).get(
            "azure_transcription_client", {}
        )
        return AzureTranscriptionClient(**azure_config_dict)

    def __init__(self, **kwargs):
        self.speech_key = kwargs.get("speech_key")
        self.service_region = kwargs.get("service_region")

        self.speech_config = speech.SpeechConfig(
            subscription=self.speech_key, region=self.service_region
        )

    def generate_transcription(self, language, source_file_path):
        try:
            result = self.speech_to_text(source_file_path, language)
        except RuntimeError as error:
            raise AzureTranscriptionClientError(error)
        return result.text

    # R1710: Either all return statements in a function should return an expression, or none
    # of them should. (inconsistent-return-statements)
    def speech_to_text(self, audio_file_path, language):
        audio_input = speech.audio.AudioConfig(filename=audio_file_path)

        LOGGER.info("calling azure stt API for file: %s", audio_file_path)
        speech_recognizer = speech.SpeechRecognizer(
            speech_config=self.speech_config,
            language=language,
            audio_config=audio_input,
        )
        LOGGER.info("Recognizing first result...")

        result = speech_recognizer.recognize_once()

        # R1705: Unnecessary "elif" after "return" (no-else-return)
        if result.reason == speech.ResultReason.RecognizedSpeech:
            LOGGER.info("Recognized: %s", result.text)
            return result
        elif result.reason == speech.ResultReason.NoMatch:
            msg = "No speech could be recognized: {}".format(
                result.no_match_details)
            raise RuntimeError(msg)
        elif result.reason == speech.ResultReason.Canceled:
            cancellation_details = result.cancellation_details
            msg = "Speech Recognition canceled: {}".format(
                cancellation_details.reason)
            raise RuntimeError(msg)
        LOGGER.info("done..")
