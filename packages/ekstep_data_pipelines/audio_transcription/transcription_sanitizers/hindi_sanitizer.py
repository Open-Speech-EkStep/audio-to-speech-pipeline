import re

from ekstep_data_pipelines.audio_transcription.transcription_sanitizers import (
    BaseTranscriptionSanitizer,
)
from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.audio_transcription_errors import (
    TranscriptionSanitizationError,
)
from ekstep_data_pipelines.common.utils import get_logger

LOGGER = get_logger("HindiTranscriptionSanitizer")


class HindiSanitizer(BaseTranscriptionSanitizer):
    VALID_CHARS = "[ ँ-ःअ-ऋए-ऑओ-नप-रलव-ह़ा-ृे-ॉो-्0-9क़-य़ ॅ]"
    PUNCTUATION = "!\"#%&'()*+,./;<=>?@[\\]^_`{|}~।"

    @staticmethod
    def get_instance(**kwargs):
        return HindiSanitizer()

    def __init__(self, *agrs, **kwargs):
        pass

    def sanitize(self, transcription: str):
        LOGGER.info("Sanitizing transcription:%s", transcription)
        transcription = (
            transcription.strip()
        )  # removes spaces from the starting and ending of transcription

        if ":" in transcription:
            raise TranscriptionSanitizationError("transcription has :")

        transcription = self.replace_bad_char(transcription)
        transcription = transcription.strip()

        if len(transcription) == 0:
            raise TranscriptionSanitizationError("transcription is empty")

        if self.shouldReject(transcription):
            raise TranscriptionSanitizationError(
                "transcription has char which is not in ँ-ःअ-ऋए-ऑओ-नप-रलव-ह़ा-ृे-ॉो-्0-9क़-य़ ॅ"
            )

        return transcription

    def shouldReject(self, transcription):
        rejected_string = re.sub(
            pattern=HindiSanitizer.VALID_CHARS, repl="", string=transcription
        )
        if len(rejected_string.strip()) > 0:
            return True

        return False

    def replace_bad_char(self, transcription):

        if "-" in transcription:
            transcription = transcription.replace("-", " ")

        table = str.maketrans(dict.fromkeys(HindiSanitizer.PUNCTUATION))
        return transcription.translate(table)
