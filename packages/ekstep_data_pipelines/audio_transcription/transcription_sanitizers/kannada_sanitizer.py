from ekstep_data_pipelines.audio_transcription.transcription_sanitizers import BaseTranscriptionSanitizer
from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.audio_transcription_errors import TranscriptionSanitizationError

from ekstep_data_pipelines.common.utils import get_logger

LOGGER = get_logger('KannadaTranscriptionSanitizer')


class KannadaSanitizer(BaseTranscriptionSanitizer):


    @staticmethod
    def get_instance(**kwargs):
        return KannadaSanitizer()

    def __init__(self, *args, **kwargs):
        pass


    def sanitize(self, transcription):
        LOGGER.info("Sanitizing transcription:" + transcription)
        transcription = transcription.strip()

        if len(transcription) == 0:
            raise TranscriptionSanitizationError('transcription is empty')

        return transcription