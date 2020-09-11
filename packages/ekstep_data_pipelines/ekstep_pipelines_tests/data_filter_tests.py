from unittest.mock import Mock, patch, call
import unittest
import sys

from data_marker.data_filter import DataFilter

sys.path.insert(0, '..')


class DataMarkerTests(unittest.TestCase):

    def setUp(self):
        pass


    def test__should_filter_by_source(self):
        # speaker_id, clipped_utterance_file_name, clipped_utterance_duration, audio_id, snr
        utterances = [
            (1, 'file_1.wav', '10', '2010123', 13),
            (2, 'file_2.wav', '11', '2010124', 11),
            (3, 'file_3.wav', '12', '2010125', 18),
            (4, 'file_4.wav', '13', '2010126', 19),
            (4, 'file_4.wav', '13', '2010126', 24)
        ]
        data_filter = DataFilter()
        filtered = list(data_filter.by_snr(utterances, {'gte': 18, 'lte': 19}))
        expected_utterances = [
            (3, 'file_3.wav', '12', '2010125', 18),
            (4, 'file_4.wav', '13', '2010126', 19)
        ]
        self.assertEqual(expected_utterances, filtered)


    def test__should_filter_by_duration(self):
        # speaker_id, clipped_utterance_file_name, clipped_utterance_duration, audio_id, snr
        utterances = [
            (1, 'file_1.wav', 4, '2010123', 13),
            (2, 'file_2.wav', 4, '2010124', 11),
            (3, 'file_3.wav', 2, '2010125', 18),
            (4, 'file_4.wav', 4, '2010126', 19),
            (4, 'file_4.wav', 2, '2010126', 24)
        ]
        data_filter = DataFilter()
        filtered = list(data_filter.by_duration(utterances, {'duration': 6}))
        expected_utterances = [
            (1, 'file_1.wav', 4, '2010123', 13),
            (2, 'file_2.wav', 4, '2010124', 11)
        ]
        self.assertEqual(expected_utterances, filtered)
