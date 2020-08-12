import sys
sys.path.insert(0, '..')

import unittest
from unittest.mock import Mock, patch
from data_marker.data_marker import DataMarker
from data_marker import constants
from sqlalchemy import text


class DataMarkerTests(unittest.TestCase):

    def setUp(self):
        self.mock_data_processor = Mock()
        self.mock_gcs_instance = Mock()

        self.data_marker = DataMarker(self.mock_data_processor, self.mock_gcs_instance)


    def test__given_config_where_process_mode_is_1_and_source_name_is_not_given__when_generate_query_invoked__then_return_correct_query(self):
        query_1, query_2 = self.data_marker.generate_query(1, None)

        expected_query_1 = constants.SELECT_SPEAKER_FOR_DATA_GREATER_THAN_DURATION_QUERY
        expected_query_2 = constants.SELECT_SPEAKER_FOR_DATA_LESS_THAN_DURATION_QUERY

        self.assertEqual(query_1.text, expected_query_1)
        self.assertEqual(query_2.text, expected_query_2)

    def test__given_config_where_process_mode_is_1_and_source_name_is_given__when_generate_query_invoked__then_return_correct_query(self):
        query_1, query_2 = self.data_marker.generate_query(1, "test_source")

        expected_query_1 = constants.SELECT_SPEAKER_FOR_DATA_GREATER_THAN_DURATION_WITH_SOURCE_QUERY
        expected_query_2 = constants.SELECT_SPEAKER_FOR_DATA_LESS_THAN_DURATION_WITH_SOURCE_QUERY

        self.assertEqual(query_1.text, expected_query_1)
        self.assertEqual(query_2.text, expected_query_2)

    def test__given_config_where_process_mode_is_2_and_source_name_is_given__when_generate_query_invoked__then_return_correct_query(self):
        query_1, query_2 = self.data_marker.generate_query(2, "test_source")

        expected_query_1 = constants.SELECT_SPEAKER_FOR_DATA_GREATER_THAN_DURATION_WITH_SOURCE_QUERY

        self.assertEqual(query_1.text, expected_query_1)
        self.assertIsNone(query_2)

    def test__given_config_where_process_mode_is_2_and_source_name_is_not_given__when_generate_query_invoked__then_return_correct_query(self):
        query_1, query_2 = self.data_marker.generate_query(2, None)

        expected_query_1 = constants.SELECT_SPEAKER_FOR_DATA_GREATER_THAN_DURATION_QUERY

        self.assertEqual(query_1.text, expected_query_1)
        self.assertIsNone(query_2)

    def test__given_config_where_process_mode_is_3_and_source_name_is_not_given__when_generate_query_invoked__then_return_correct_query(self):
        query_1, query_2 = self.data_marker.generate_query(3, None)

        expected_query_1 = constants.SELECT_SPEAKER_FOR_DATA_LESS_THAN_DURATION_QUERY

        self.assertEqual(query_1.text, expected_query_1)
        self.assertIsNone(query_2)

    def test__given_config_where_process_mode_is_3_and_source_name_is_given__when_generate_query_invoked__then_return_correct_query(self):
        query_1, query_2 = self.data_marker.generate_query(3, "test_source")

        expected_query_1 = constants.SELECT_SPEAKER_FOR_DATA_LESS_THAN_DURATION_WITH_SOURCE_QUERY

        self.assertEqual(query_1.text, expected_query_1)
        self.assertIsNone(query_2)

