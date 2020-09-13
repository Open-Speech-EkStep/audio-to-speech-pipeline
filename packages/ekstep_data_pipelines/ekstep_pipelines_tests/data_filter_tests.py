import unittest
import sys

from data_marker.data_filter import DataFilter

sys.path.insert(0, '..')


class DataMarkerTests(unittest.TestCase):

    def setUp(self):
        pass


    def test__should_filter_by_snr(self):
        # speaker_id, clipped_utterance_file_name, clipped_utterance_duration, audio_id, snr
        utterances = [
            (1, 'file_1.wav', 10, 202009112003117071, 13),
            (2, 'file_2.wav', 11, 202009112003117071, 11),
            (3, 'file_3.wav', 12, 202009112003117071, 45),
            (4, 'file_4.wav', 13, 202009112003117071, 19),
            (4, 'file_4.wav', 6, 202009112003117071, 21),
            (4, 'file_4.wav', 13, 202009112003117071, 100)
        ]
        data_filter = DataFilter()
        filtered = list(data_filter.by_snr(utterances, {'gte': 15, 'lte': 50}))
        expected_utterances = [
            (3, 'file_3.wav', 12, 202009112003117071, 45),
            (4, 'file_4.wav', 13, 202009112003117071, 19),
            (4, 'file_4.wav', 6, 202009112003117071, 21),
        ]
        print(type(filtered[0][3]))
        self.assertEqual(expected_utterances, filtered)


    def test__should_filter_by_duration(self):
        # speaker_id, clipped_utterance_file_name, clipped_utterance_duration, audio_id, snr
        utterances = [
            (1, 'file_1.wav', 4, '2010123', 13),
            (2, 'file_2.wav', 2, '2010124', 11),
            (3, 'file_3.wav', 4, '2010125', 18),
            (4, 'file_4.wav', 4, '2010126', 19),
            (4, 'file_4.wav', 2, '2010126', 24)
        ]
        data_filter = DataFilter()
        filtered = data_filter.by_duration(utterances, 7/3600)
        expected_utterances = [
            (1, 'file_1.wav', 4, '2010123', 13),
            (2, 'file_2.wav', 2, '2010124', 11)
        ]
        print(str(expected_utterances))
        self.assertEqual(expected_utterances, filtered)

    def test__should_filter_by_duration_with_randomness(self):
        # speaker_id, clipped_utterance_file_name, clipped_utterance_duration, audio_id, snr
        utterances = [
            (1, 'file_1.wav', 4, '2010123', 13),
            (2, 'file_2.wav', 2, '2010124', 11),
            (3, 'file_3.wav', 4, '2010125', 18),
            (4, 'file_4.wav', 4, '2010126', 19),
            (4, 'file_4.wav', 2, '2010126', 24),
            (4, 'file_4.wav', 2, '2010126', 24),
            (5, 'file_4.wav', 2, '2010126', 24),
            (5, 'file_4.wav', 2, '2010126', 24),
            (6, 'file_4.wav', 2, '2010126', 24),
            (6, 'file_4.wav', 2, '2010126', 24),
            (6, 'file_4.wav', 2, '2010126', 24)
        ]
        data_filter = DataFilter()
        filtered1 = data_filter.by_duration(utterances, 7/3600, True, 1)
        filtered2 = data_filter.by_duration(utterances, 7/3600, True, 1)
        expected_utterances = [
            (1, 'file_1.wav', 4, '2010123', 13),
            (2, 'file_2.wav', 2, '2010124', 11)
        ]
        print(str(filtered1))
        print(str(filtered2))
        self.assertNotEqual(expected_utterances, filtered1)
        self.assertNotEqual(filtered1, filtered2)

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
        filtered = list(data_filter.by_per_speaker_duration(utterances, {'lte_per_speaker_duration': 8/60, 'gte_per_speaker_duration': 0, 'with_threshold': 2/60}))
        expected_utterances = [
            (1, 'file_10.wav', 4, '1', 13),
            (1, 'file_11.wav', 1, '2', 13),
            (1, 'file_12.wav', 2, '3', 13),
            (2, 'file_2.wav', 4, '5', 11),
            (3, 'file_3.wav', 2, '6', 18),
            (3, 'file_4.wav', 4, '7', 18),
            (4, 'file_5.wav', 4, '8', 19),
            (4, 'file_6.wav', 4, '9', 24),
            (5, 'file_50.wav', 5, '10', 25),
            (5, 'file_51.wav', 4, '10', 26),
            (6, 'file_53.wav', 5, '10', 25),
            (6, 'file_54.wav', 4, '10', 26),
        ]
        self.assertEqual(type(expected_utterances), type(filtered))  # check they are the same type
        self.assertEqual(len(expected_utterances), len(filtered))  # check they are the same length
        self.assertEqual(expected_utterances, filtered)


    def test__should_apply_filters_only_source(self):
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

        filters = {
            'by_source': 'swayamprabha_chapter_30',
        }

        expected_utterances = [
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
        filtered = data_filter.apply_filters(filters, utterances)
        self.assertEqual(type(expected_utterances), type(filtered))  # check they are the same type
        self.assertEqual(len(expected_utterances), len(filtered))  # check they are the same length
        self.assertEqual(expected_utterances, filtered)

    def test__should_apply_filters_by_source_and_then_by_snr(self):
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

        filters = {
            'by_source': 'swayamprabha_chapter_30',
            'then_by_snr': {'gte': 13, 'lte': 26}
        }

        expected_utterances = [
            (1, 'file_10.wav', 4, '1', 13),
            (1, 'file_11.wav', 1, '2', 13),
            (1, 'file_12.wav', 2, '3', 13),
            (1, 'file_13.wav', 4, '4', 13),
            (3, 'file_3.wav', 2, '6', 18),
            (3, 'file_4.wav', 4, '7', 18),
            (4, 'file_5.wav', 4, '8', 19),
            (4, 'file_6.wav', 4, '9', 24),
            (4, 'file_7.wav', 4, '10', 24),
            (5, 'file_50.wav', 5, '10', 25),
            (5, 'file_51.wav', 4, '10', 26),
            (6, 'file_53.wav', 5, '10', 25),
            (6, 'file_54.wav', 4, '10', 26),
        ]
        data_filter = DataFilter()
        filtered = data_filter.apply_filters(filters, utterances)
        self.assertEqual(type(expected_utterances), type(filtered))  # check they are the same type
        self.assertEqual(len(expected_utterances), len(filtered))  # check they are the same length
        self.assertEqual(expected_utterances, filtered)

    def test__should_apply_filters_with_by_snr_then_by_speaker(self):
        utterances = [
            (1, 'file_10.wav', 4, '1', 13),
            (1, 'file_11.wav', 1, '2', 13),
            (1, 'file_12.wav', 2, '3', 13),
            (1, 'file_13.wav', 4, '4', 13),
            (2, 'file_2.wav', 4, '5', 11),
            (2, 'file_2.wav', 4, '5', 18),
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

        filters = {
            'by_source': 'swayamprabha_chapter_30',
            'then_by_snr': {'gte': 13, 'lte': 26},
            'then_by_speaker': {'lte_per_speaker_duration': 8/60, 'gte_per_speaker_duration': 5/60, 'with_threshold': 0}
            # 'then_by_duration': 35
        }

        expected_utterances = [
            (1, 'file_10.wav', 4, '1', 13),
            (1, 'file_11.wav', 1, '2', 13),
            (1, 'file_12.wav', 2, '3', 13),
            (3, 'file_3.wav', 2, '6', 18),
            (3, 'file_4.wav', 4, '7', 18),
            (4, 'file_5.wav', 4, '8', 19),
            (4, 'file_6.wav', 4, '9', 24),
            (5, 'file_50.wav', 5, '10', 25),
            (6, 'file_53.wav', 5, '10', 25),

        ]
        data_filter = DataFilter()
        filtered = data_filter.apply_filters(filters, utterances)
        self.assertEqual(type(expected_utterances), type(filtered))  # check they are the same type
        self.assertEqual(len(expected_utterances), len(filtered))  # check they are the same length
        self.assertEqual(expected_utterances, filtered)

    def test__should_apply_filters_with_by_snr_then_by_speaker_then_by_duration(self):
        utterances = [
            (1, 'file_10.wav', 4, '1', 13),
            (1, 'file_11.wav', 1, '2', 13),
            (1, 'file_12.wav', 2, '3', 13),
            (1, 'file_13.wav', 4, '4', 13),
            (2, 'file_2.wav', 4, '5', 11),
            (2, 'file_2.wav', 4, '5', 18),
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

        filters = {
            'by_source': 'swayamprabha_chapter_30',
            'then_by_snr': {'gte': 13, 'lte': 26},
            'then_by_speaker': {'lte_per_speaker_duration': 8/60, 'gte_per_speaker_duration': 5/60, 'with_threshold': 0},
            'then_by_duration': 21/3600
        }

        expected_utterances = [
            (1, 'file_10.wav', 4, '1', 13),
            (1, 'file_11.wav', 1, '2', 13),
            (1, 'file_12.wav', 2, '3', 13),
            (3, 'file_3.wav', 2, '6', 18),
            (3, 'file_4.wav', 4, '7', 18),
            (4, 'file_5.wav', 4, '8', 19),
            (4, 'file_6.wav', 4, '9', 24),
        ]
        data_filter = DataFilter()
        filtered = data_filter.apply_filters(filters, utterances)
        self.assertEqual(type(expected_utterances), type(filtered))  # check they are the same type
        self.assertEqual(len(expected_utterances), len(filtered))  # check they are the same length
        self.assertEqual(expected_utterances, filtered)

    def test__should_apply_filters_with_by_snr_then_by_duration(self):
        utterances = [
            (1, 'file_10.wav', 4, '1', 13),
            (1, 'file_11.wav', 1, '2', 13),
            (1, 'file_12.wav', 2, '3', 13),
            (1, 'file_13.wav', 4, '4', 13),
            (2, 'file_2.wav', 4, '5', 11),
            (2, 'file_2.wav', 4, '5', 18),
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

        filters = {
            'by_source': 'swayamprabha_chapter_30',
            'then_by_snr': {'gte': 13, 'lte': 26},
            'then_by_duration': 21/3600
        }

        expected_utterances = [
            (1, 'file_10.wav', 4, '1', 13),
            (1, 'file_11.wav', 1, '2', 13),
            (1, 'file_12.wav', 2, '3', 13),
            (1, 'file_13.wav', 4, '4', 13),
            (2, 'file_2.wav', 4, '5', 18),
            (3, 'file_3.wav', 2, '6', 18),
            (3, 'file_4.wav', 4, '7', 18),
        ]
        data_filter = DataFilter()
        filtered = data_filter.apply_filters(filters, utterances)
        self.assertEqual(type(expected_utterances), type(filtered))  # check they are the same type
        self.assertEqual(len(expected_utterances), len(filtered))  # check they are the same length
        self.assertEqual(expected_utterances, filtered)

    def test__should_apply_filters_with_by_snr_then_by_duration_randomness(self):
        utterances = [
            (1, 'file_10.wav', 4, '1', 13),
            (1, 'file_11.wav', 1, '2', 13),
            (1, 'file_12.wav', 2, '3', 13),
            (1, 'file_13.wav', 4, '4', 13),
            (2, 'file_2.wav', 4, '5', 11),
            (2, 'file_2.wav', 4, '5', 18),
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

        filters = {
            'by_source': 'swayamprabha_chapter_30',
            'then_by_snr': {'gte': 13, 'lte': 26},
            'then_by_duration': 21,
            'with_randomness': True
        }

        expected_utterances = [
            (1, 'file_10.wav', 4, '1', 13),
            (1, 'file_11.wav', 1, '2', 13),
            (1, 'file_12.wav', 2, '3', 13),
            (1, 'file_13.wav', 4, '4', 13),
            (2, 'file_2.wav', 4, '5', 18),
            (3, 'file_3.wav', 2, '6', 18),
            (3, 'file_4.wav', 4, '7', 18),
        ]
        data_filter = DataFilter()
        filtered1 = data_filter.apply_filters(filters, utterances)
        filtered2 = data_filter.apply_filters(filters, utterances)
        self.assertEqual(type(expected_utterances), type(filtered1))  # check they are the same type
        self.assertNotEqual(expected_utterances, filtered1)
        self.assertNotEqual(expected_utterances, filtered1)
        self.assertNotEqual(expected_utterances, filtered2)
        self.assertNotEqual(filtered1, filtered2)

    def test__should_apply_filters_with_source_then_by_duration(self):
        utterances = [
            (1, 'file_10.wav', 4, '1', 13),
            (1, 'file_11.wav', 1, '2', 13),
            (1, 'file_12.wav', 2, '3', 13),
            (1, 'file_13.wav', 4, '4', 13),
            (2, 'file_2.wav', 4, '5', 11),
            (2, 'file_2.wav', 4, '5', 18),
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

        filters = {
            'by_source': 'swayamprabha_chapter_30',
            'then_by_duration': 21/3600
        }

        expected_utterances = [
            (1, 'file_10.wav', 4, '1', 13),
            (1, 'file_11.wav', 1, '2', 13),
            (1, 'file_12.wav', 2, '3', 13),
            (1, 'file_13.wav', 4, '4', 13),
            (2, 'file_2.wav', 4, '5', 11),
            (2, 'file_2.wav', 4, '5', 18),
            (3, 'file_3.wav', 2, '6', 18)
        ]
        data_filter = DataFilter()
        filtered = data_filter.apply_filters(filters, utterances)
        self.assertEqual(type(expected_utterances), type(filtered))  # check they are the same type
        self.assertEqual(len(expected_utterances), len(filtered))  # check they are the same length
        self.assertEqual(expected_utterances, filtered)
