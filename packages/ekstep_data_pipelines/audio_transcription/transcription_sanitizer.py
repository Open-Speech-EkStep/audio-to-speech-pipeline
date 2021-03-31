import re

from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.audio_transcription_errors import (
    TranscriptionSanitizationError, )
from ekstep_data_pipelines.common.utils import get_logger

LOGGER = get_logger("TranscriptionSanitizer")


class TranscriptionSanitizer(object):
    def sanitize(self, transcription):
        LOGGER.info("Sanitizing transcription:" + transcription)
        transcription = (
            transcription.strip()
        )  # removes spaces in starting of transcription
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
        valid_char = "[ ँ-ःअ-ऋए-ऑओ-नप-रलव-ह़ा-ृे-ॉो-्0-9क़-य़ ॅ]"
        rejected_string = re.sub(
            pattern=valid_char,
            repl="",
            string=transcription)
        if len(rejected_string.strip()) > 0:
            return True

        return False

    def replace_bad_char(self, transcription):

        if "-" in transcription:
            transcription = transcription.replace("-", " ")

        punctuation = "!\"#%&'()*+,./;<=>?@[\\]^_`{|}~।"
        table = str.maketrans(dict.fromkeys(punctuation))
        return transcription.translate(table)
