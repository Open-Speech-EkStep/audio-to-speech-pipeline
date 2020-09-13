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
            (1, 'file_1.wav', 10, '2010123', 13),
            (2, 'file_2.wav', 11, '2010124', 11),
            (3, 'file_3.wav', 12, '2010125', 18),
            (4, 'file_4.wav', 13, '2010126', 19),
            (4, 'file_4.wav', 6, '2010126', 21),
            (4, 'file_4.wav', 13, '2010126', 24)
        ]
        data_filter = DataFilter()
        filtered = list(data_filter.by_snr(utterances, {'gte_snr': 18, 'lte_snr': 21, 'total_duration': 24}))
        expected_utterances = [
            (3, 'file_3.wav', 12, '2010125', 18),
            (4, 'file_4.wav', 13, '2010126', 19)
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
        filtered = list(data_filter.by_duration(utterances, {'total_duration': 6}))
        expected_utterances = [
            (1, 'file_1.wav', 4, '2010123', 13),
            (2, 'file_2.wav', 4, '2010124', 11)
        ]
        self.assertEqual(expected_utterances, filtered)

    def test__should_filter_by_speaker_duration(self):
        # speaker_id, clipped_utterance_file_name, clipped_utterance_duration, audio_id, snr
        utterances = [
            (1, 'file_10.wav', 4, '1', 13),
            (1, 'file_11.wav', 1, '2', 13),
            (1, 'file_12.wav', 2, '3', 13),
            (1, 'file_13.wav', 4, '4', 13),
            (2, 'file_2.wav', 4, '5', 11),
            (3, 'file_3.wav', 2, '6', 18),
            (3, 'file_4.wav', 4, '7', 18),
            (4, 'file_5.wav', 4, '8', 19),
            (4, 'file_6.wav', 4, '9', 24),
            (4, 'file_7.wav', 4, '10', 24),
            (5, 'file_50.wav', 5, '10', 25),
            (5, 'file_51.wav', 4, '10', 26),
            (5, 'file_52.wav', 5, '10', 27),
            (6, 'file_53.wav', 5, '10', 25),
            (6, 'file_54.wav', 4, '10', 26),
            (6, 'file_55.wav', 5, '10', 27)
        ]
        data_filter = DataFilter()
        filtered = list(data_filter.by_per_speaker_duration(utterances, {'total_duration': 34, 'lte_per_speaker_duration': 8, 'gte_per_speaker_duration': 0, 'threshold': 2}))
        expected_utterances = [
            (1, 'file_10.wav', 4, '1', 13),
            (1, 'file_11.wav', 1, '2', 13),
            (1, 'file_12.wav', 2, '3', 13),
            (2, 'file_2.wav', 4, '5', 11),
            (3, 'file_3.wav', 2, '2', 18),
            (3, 'file_4.wav', 4, '4', 18),
            (4, 'file_5.wav', 4, '8', 19),
            (4, 'file_6.wav', 4, '9', 24),
            (5, 'file_50.wav', 5, '10', 25),
            (5, 'file_51.wav', 4, '10', 26),
            (6, 'file_53.wav', 5, '10', 25)
        ]
        self.assertEqual(type(expected_utterances), type(filtered))  # check they are the same type
        self.assertEqual(len(expected_utterances), len(filtered))  # check they are the same length
        self.assertEqual(expected_utterances[1][0], filtered[1][0])
        self.assertEqual(expected_utterances[6][0], filtered[6][0])
        self.assertEqual(expected_utterances[6][2], filtered[6][2])
