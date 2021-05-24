import unittest

from ekstep_data_pipelines.audio_transcription.transcription_sanitizers import (
    get_transcription_sanitizers,
)
from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.audio_transcription_errors import (
    TranscriptionSanitizationError,
)


class TestTrancriptionSanitizer(unittest.TestCase):
    def setUp(self):
        transcription_sanitizers = get_transcription_sanitizers()
        self.gujarati_transcription_sanitizers = transcription_sanitizers.get("gujarati")

    def test_transcription_containing_empty_string_should_raise_runtime_exception(self):
        transcript_obj = self.gujarati_transcription_sanitizers
        transcript = " "
        with self.assertRaises(TranscriptionSanitizationError):
            transcript_obj.sanitize(transcription=transcript)

    def test_transcription_containing_space_in_start_should_return_None(self):
        transcript_obj = self.gujarati_transcription_sanitizers
        transcript = " તમે ક્યાં જાવ છો"
        self.assertEqual(
            transcript_obj.sanitize(transcript), "તમે ક્યાં જાવ છો"
        )

    def test_transcription_containing_english_character_should_give_runtime_exception(
        self,
    ):
        transcript_obj = self.gujarati_transcription_sanitizers
        transcriptions = "4K તમે ક્યાં જાવ છો"
        self.assertEqual(transcript_obj.shouldReject(transcriptions), True)

    def test_transcription_containing_other_character_should_give_runtime_exception(
        self,
    ):
        transcript_obj = self.gujarati_transcription_sanitizers
        transcriptions = "अलग अलग dummy તમે ક્યાં જાવ છો"
        self.assertEqual(transcript_obj.shouldReject(transcriptions), True)


if __name__ == "__main__":
    unittest.main()
