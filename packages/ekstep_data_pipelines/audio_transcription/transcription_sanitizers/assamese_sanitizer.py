import re

from ekstep_data_pipelines.audio_transcription.transcription_sanitizers import (
    BaseTranscriptionSanitizer,
)
from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.audio_transcription_errors import (
    TranscriptionSanitizationError,
)
from ekstep_data_pipelines.common.utils import get_logger

LOGGER = get_logger("AssameseTranscriptionSanitizer")


class AssameseSanitizer(BaseTranscriptionSanitizer):
    VALID_CHARS = "[ ঁ-ঃঅ-ঋএ-ঐও-চচ-নপ-যলশ-হা-ৃে-ৈো-ৎৗড়-ঢ়য়-ৠৰ-ৱ৺]+"
    PUNCTUATION = "!\"#%&'()*+,./;<=>?@[\\]^_`{|}~।"

    @staticmethod
    def get_instance(**kwargs):
        return AssameseSanitizer()

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
                "transcription has char which is not in ঁ-ঃঅ-ঋএ-ঐও-চচ-নপ-যলশ-হা-ৃে-ৈো-ৎৗড়-ঢ়য়-ৠৰ-ৱ৺"
            )

        return transcription

    def shouldReject(self, transcription):
        rejected_string = re.sub(
            pattern=AssameseSanitizer.VALID_CHARS, repl="", string=transcription
        )

        if len(rejected_string.strip()) > 0:
            return True

        return False

    def replace_bad_char(self, transcription):

        LOGGER.info("replace panctuation if present ")

        if "-" in transcription:
            transcription = transcription.replace("-", " ")

        table = str.maketrans(dict.fromkeys(AssameseSanitizer.PUNCTUATION))
        return transcription.translate(table)
