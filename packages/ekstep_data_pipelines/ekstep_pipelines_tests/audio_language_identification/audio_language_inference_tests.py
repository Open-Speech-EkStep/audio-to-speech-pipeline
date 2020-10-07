import sys
import unittest

from audio_language_identification import audio_language_inference

sys.path.insert(0, '..')


class AudioLanguageIdentificationTests(unittest.TestCase):

    def test_language_inference(self):
        model_path = 'ekstep_pipelines_tests/audio_language_identification/model.pt'
        audio_path = 'ekstep_pipelines_tests/resources/chunk.wav'
        confidence_score = audio_language_inference.evaluation(audio_path, model_path)
        print(confidence_score)
        expected_score = ['0.00004', '0.99996']
        self.assertEquals(expected_score, confidence_score)


    def test_language_confidence_score_map(self):
        confidence_scores = ['0.00004', '0.99996']
        language_map_path = 'ekstep_pipelines_tests/audio_language_identification/language_map.yml'
        confidence_score = audio_language_inference.language_confidence_score_map(language_map_path, confidence_scores)
        print(confidence_score)
        expected_score = {"confidence_score": {"hi-IN": "0.00004", "en": "0.99996"}}
        self.assertEquals(expected_score, confidence_score)