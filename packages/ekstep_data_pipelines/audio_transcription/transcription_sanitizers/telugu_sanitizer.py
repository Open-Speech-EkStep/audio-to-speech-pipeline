import re

from ekstep_data_pipelines.audio_transcription.transcription_sanitizers import (
    BaseTranscriptionSanitizer,
)
from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.audio_transcription_errors import (
    TranscriptionSanitizationError,
)
from ekstep_data_pipelines.common.utils import get_logger

LOGGER = get_logger("TeluguTranscriptionSanitizer")


class TeluguSanitizer(BaseTranscriptionSanitizer):
    VALID_CHARS = "[ ం-ఃఅ-ఌఎ-ఐఒ-నప-ళవ-హా-ౄె-ైొ-్ౠ]+"
    PUNCTUATION = "!\"#%&'()*+,./;<=>?@[\\]^_`{|}~।"

    @staticmethod
    def get_instance(**kwargs):
        return TeluguSanitizer()

    def __init__(self, *args, **kwargs):
        pass

    def sanitize(self, transcription):
        LOGGER.info("Sanitizing transcription:%s", transcription)
        transcription = transcription.strip()

        transcription = self.replace_bad_char(transcription)

        transcription = transcription.strip()

        if len(transcription) == 0:
            raise TranscriptionSanitizationError("transcription is empty")

        if self.shouldReject(transcription):
            raise TranscriptionSanitizationError(
                "transcription has char which is not in ం-ఃఅ-ఌఎ-ఐఒ-నప-ళవ-హా-ౄె-ైొ-్ౠ"
            )

        return transcription

    def shouldReject(self, transcription):
        rejected_string = re.sub(
            pattern=TeluguSanitizer.VALID_CHARS, repl="", string=transcription
        )

        if len(rejected_string.strip()) > 0:
            return True

        return False

    def replace_bad_char(self, transcription):

        LOGGER.info("replace panctuation if present ")

        if "-" in transcription:
            transcription = transcription.replace("-", " ")

        table = str.maketrans(dict.fromkeys(TeluguSanitizer.PUNCTUATION))
        return transcription.translate(table)
