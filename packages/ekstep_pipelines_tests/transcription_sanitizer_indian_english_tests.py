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
        self.indian_english_transcription_sanitizers = transcription_sanitizers.get(
            "indian_english"
        )

    def test_transcription_containing_empty_string_should_raise_runtime_exception(self):
        transcript_obj = self.indian_english_transcription_sanitizers
        transcript = " "
        with self.assertRaises(TranscriptionSanitizationError):
            transcript_obj.sanitize(transcription=transcript)

    def test_transcription_containing_space_in_start_should_return_None(self):
        transcript_obj = self.indian_english_transcription_sanitizers
        transcript = " Good morning 123"
        self.assertEqual(transcript_obj.sanitize(transcript), "Good morning 123")

    def test_transcription_containing_english_character_should_give_runtime_exception(
        self,
    ):
        transcript_obj = self.indian_english_transcription_sanitizers
        transcriptions = "ಸಾರ್ವಜನಿಕರ Good morning"
        self.assertEqual(transcript_obj.shouldReject(transcriptions), True)

    def test_transcription_containing_other_character_should_give_runtime_exception(
        self,
    ):
        transcript_obj = self.indian_english_transcription_sanitizers
        transcriptions = "अलग अलग dummy ಸಾರ್ವಜನಿಕರ ಕುಂದು ಕೊರತೆಗಳ"
        self.assertEqual(transcript_obj.shouldReject(transcriptions), True)

    def test_transcription_containing_apostrope_should_return_transcription_with_apostrope(
        self,
    ):
        transcript_obj = self.indian_english_transcription_sanitizers
        transcript = "What's your name?"
        self.assertEqual(transcript_obj.sanitize(transcript), "What's your name")


if __name__ == "__main__":
    unittest.main()
