#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import unittest
from ekstep_data_pipelines.audio_transcription.transcription_sanitizer import TranscriptionSanitizer
from ekstep_data_pipelines.audio_transcription.audio_transcription_errors import TranscriptionSanitizationError



class TestTrancriptionSanitizer(unittest.TestCase):

    def test_transcription_containing_empty_string_should_raise_runtime_exception(self):
        transcript_obj = TranscriptionSanitizer()
        transcript = " "
        with self.assertRaises(TranscriptionSanitizationError):
            transcript_obj.sanitize(transcription=transcript)

    def test_transcription_containing_space_in_start_should_return_None(self):
        transcript_obj = TranscriptionSanitizer()
        transcript = ' अलग अलग होते हैं'
        self.assertEqual(transcript_obj.sanitize(transcript), 'अलग अलग होते हैं')

    def test_transcription_punctuations_are_being_removed(self):
        transcript_obj = TranscriptionSanitizer()
        transcript = 'अलग-अलग होते है!"#%&\'()*+,./;<=>?@[\\]^_`{|}~।'
        self.assertEqual(transcript_obj.replace_bad_char(transcript), 'अलग अलग होते है')

    def test_transcription_containing_numbers_0123456789_should_be_accepted(self):
        transcript_obj = TranscriptionSanitizer()
        transcript = 'लेकिन मैक्सिमॅम 0123456789'
        self.assertEqual(transcript_obj.shouldReject(transcript), False)

    def test_transcription_containing_english_character_should_give_runtime_exception(self):
        transcript_obj = TranscriptionSanitizer()
        transcriptions = '4K की स्पीड थी'
        self.assertEqual(transcript_obj.shouldReject(transcriptions), True)

    def test_transcription_should_pass_for_given_samples(self):
        transcript_obj = TranscriptionSanitizer()
        transcripts = [("अलग-अलग होते हैं ", 'अलग अलग होते हैं'),
                       ("इफ यू हॅव ठीक थी", 'इफ यू हॅव ठीक थी'), ("डिस्कॅशंस", 'डिस्कॅशंस'),
                       ("लेकिन मैक्सिमॅम ", 'लेकिन मैक्सिमॅम'), ("फ्लैट चलाते-चलाते", 'फ्लैट चलाते चलाते'),
                       ("1126 वॅन", '1126 वॅन'),
                       ("दो बच्चे हो गए दोनों का दो-दो बच्चे", 'दो बच्चे हो गए दोनों का दो दो बच्चे'),
                       ("कॅन्फ़्यूज़न हो जाता है कि मैं कौनसा लू ", 'कॅन्फ़्यूज़न हो जाता है कि मैं कौनसा लू')]
        for each_transcript, correct_response in transcripts:
            self.assertEqual(transcript_obj.sanitize(each_transcript), correct_response)

    def test_transcription_containing_time_should_fail(self):
        transcript_obj = TranscriptionSanitizer()
        with self.assertRaises(TranscriptionSanitizationError):
            transcript_obj.sanitize(transcription="8:00 से")

    def test_transcription_should_fail_for_given_samples(self):
        transcript_obj = TranscriptionSanitizer()
        transcriptions = ["8:00 से", "टेक्स्ट टू दीपा वन ऍफ़ टू", "रजिस्ट्री ऍफ़", "3dmili", "x-ray निकाल के दिखाते हैं ",
                          "e-filing आ जाती है ", "B.Ed कॉलेज ", "m.a. B.Ed पूरी कर ",
                          "दिनभर patient-centered ", "₹300", "$500"]
        for each_transcription in transcriptions:
            with self.assertRaises(TranscriptionSanitizationError):
                transcript_obj.sanitize(each_transcription)


if __name__ == '__main__':
    unittest.main()
