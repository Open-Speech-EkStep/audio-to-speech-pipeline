from ekstep_data_pipelines.audio_transcription.transcription_sanitizers import BaseTranscriptionSanitizer
from ekstep_data_pipelines.common.utils import get_logger

LOGGER = get_logger('GujratiTranscriptionSanitizer')


class GujratiSanitizer(BaseTranscriptionSanitizer):


    @staticmethod
    def get_instance(**kwargs):
        return GujratiSanitizer()

    def __init__(self, *args, **kwargs):
        pass


    def sanitize(self, transcription):
        pass