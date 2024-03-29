from ekstep_data_pipelines.audio_transcription.transcription_sanitizers import (
    BaseTranscriptionSanitizer,
)
from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.audio_transcription_errors import (
    TranscriptionSanitizationError,
)

from ekstep_data_pipelines.common.utils import get_logger
import re

LOGGER = get_logger("IndianEnglishSanitizer")


class IndianEnglishSanitizer(BaseTranscriptionSanitizer):

    VALID_CHARS = "[ a-zA-Z0-9']"
    PUNCTUATION = '!"#%&()*+,./;<=>?@[\\]^_`{|}~।'

    @staticmethod
    def get_instance(**kwargs):
        return IndianEnglishSanitizer()

    def __init__(self, *args, **kwargs):
        pass

    def sanitize(self, transcription):
        LOGGER.info("Sanitizing transcription:" + transcription)
        transcription = transcription.strip()

        transcription = self.replace_bad_char(transcription)

        transcription = transcription.strip()

        if len(transcription) == 0:
            raise TranscriptionSanitizationError("transcription is empty")

        if self.shouldReject(transcription):
            raise TranscriptionSanitizationError(
                "transcription has char which is not in a-zA-Z0-9'"
            )

        return transcription

    def shouldReject(self, transcription):
        rejected_string = re.sub(
            pattern=IndianEnglishSanitizer.VALID_CHARS, repl="", string=transcription
        )

        if len(rejected_string.strip()) > 0:
            return True

        return False

    def replace_bad_char(self, transcription):

        LOGGER.info("replace panctuation if present ")

        if "-" in transcription:
            transcription = transcription.replace("-", " ")

        table = str.maketrans(dict.fromkeys(IndianEnglishSanitizer.PUNCTUATION))
        return transcription.translate(table)
