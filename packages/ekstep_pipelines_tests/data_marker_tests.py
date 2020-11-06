import json
import unittest
import sys
from unittest.mock import Mock
from ekstep_data_pipelines.data_marker.data_marker import DataMarker



class DataMarkerTests(unittest.TestCase):

    def setUp(self):
        self.postgres_client = Mock()
        self.gcp_instance = Mock()
        self.data_stager = DataMarker(self.postgres_client, self.gcp_instance )


    def test__should_transform_utterances_to_file_paths(self):
        source_base_path = 'gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/catalogued/hindi/audio/source_1'
        utterances = [
            (1, 'file_10.wav', 4, '2020123', 13),
            (1, 'file_11.wav', 1, '2020124', 14)
        ]
        files = self.data_stager.to_files(utterances, source_base_path)

        expected_files = [
            'gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/catalogued/hindi/audio/source_1/2020123/clean/file_10.wav',
            'gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/catalogued/hindi/audio/source_1/2020124/clean/file_11.wav'
        ]
        self.assertListEqual(files, expected_files)

    def test__should_parse_config(self):
        data_marker_config = '{ "swayamprabha_chapter_4": { "filter": { "then_by_snr": { "lte": 100, "gte": 15 }, "then_by_duration": 1, "with_randomness": "true" } } }'

        filteg_config = json.loads(data_marker_config)
        key = list(filteg_config.keys())[0]
        kwargs = {"filter": filteg_config.get(key), "source": key}
        source, filter_criteria = self.data_stager.get_config(**kwargs)
        self.assertEqual(source, "swayamprabha_chapter_4")
        self.assertEqual(filter_criteria, { "then_by_snr": { "lte": 100, "gte": 15 }, "then_by_duration": 1, "with_randomness": "true"} )

        by_snr = filter_criteria.get('then_by_snr', None)
        by_duration = filter_criteria.get('then_by_duration', None)
        with_randomness = filter_criteria.get('with_randomness', 'false')

        self.assertEqual(by_snr, { "lte": 100, "gte": 15 })
        self.assertEqual(by_duration, 1)