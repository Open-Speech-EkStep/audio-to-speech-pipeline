import unittest
import sys
from unittest.mock import Mock
from data_marker.data_marker import DataMarker

sys.path.insert(0, '..')


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
