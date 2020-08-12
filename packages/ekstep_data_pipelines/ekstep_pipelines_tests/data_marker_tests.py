import sys
sys.path.insert(0, '..')

import unittest
from unittest.mock import Mock, patch, call
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

    def tests__given_speaker_criteria_with_duration_missing__when_get_speakers_with_source_duration_invoked__then_return_none(self):
        speaker_criteria = {
            constants.DURATION: 30,
            constants.NUMBER_OF_SPEAKERS: 10
        }
        actual_output = self.data_marker.get_speakers_with_source_duration(speaker_criteria)
        self.assertIsNone(actual_output)
        self.assertEqual(self.mock_data_processor.connection.execute.call_count, 0)

    # TODO: Need to do this
    def tests__given_speaker_criteria_with_all_required_fields__when_get_speakers_with_source_duration_invoked__then_return_speaker_dict(self):
        pass

    def test__given_valid_speaker_criteria_where_source_name_specified__when_get_speaker_name_list_invoked__then_return_speaker_name_list(self):
        speaker_criteria = {
            constants.DURATION: 30,
            constants.NUMBER_OF_SPEAKERS: 10,
            constants.SOURCE_NAME: "test_source",
            constants.PROCESS_MODE: 1
        }

        greater_than_duration_speakers = [('speaker_greater_than_duration_1',), ('speaker_greater_than_duration_2',),
                                          ('speaker_greater_than_duration_3',)]
        less_than_duration_speakers = [('speaker_less_than_duration_1',), ('speaker_less_than_duration_2',),
                                       ('speaker_less_than_duration_3',)]

        combined_list = greater_than_duration_speakers + less_than_duration_speakers


        query_result_mock = Mock()
        query_result_mock.fetchall.side_effect = [greater_than_duration_speakers, less_than_duration_speakers]

        self.mock_data_processor.connection.execute.return_value = query_result_mock


        speaker_name_list = ','.join([f"'{self.data_marker.escape_sql_special_char(speaker_name[0])}'"for speaker_name in combined_list])
        expected_output = f'({speaker_name_list})'

        expected_param_dict = {
            "duration": 30,
            "speaker_count": 10,
            "source_name": "test_source"
        }

        actual_output = self.data_marker._get_speaker_name_list(speaker_criteria)
        self.assertEqual(expected_output, actual_output)

        curr_args = self.mock_data_processor.connection.execute.call_args_list

        self.assertEqual(curr_args[0][0][0].text, constants.SELECT_SPEAKER_FOR_DATA_GREATER_THAN_DURATION_WITH_SOURCE_QUERY)
        self.assertDictEqual(curr_args[0][1], expected_param_dict)
        self.assertEqual(curr_args[1][0][0].text, constants.SELECT_SPEAKER_FOR_DATA_LESS_THAN_DURATION_WITH_SOURCE_QUERY)
        self.assertDictEqual(curr_args[1][1], expected_param_dict)
        self.assertEqual(len(curr_args), 2)
        self.assertEqual(self.mock_data_processor.connection.execute.call_count, 2)
        self.assertEqual(query_result_mock.fetchall.call_count, 2)


    def test__given_valid_speaker_criteria_where_source_name_not_specified__when_get_speaker_name_list_invoked__then_return_speaker_name_list(self):
        speaker_criteria = {
            constants.DURATION: 30,
            constants.NUMBER_OF_SPEAKERS: 10,
            constants.PROCESS_MODE: 2
        }

        speakers = [('speaker_1',), ('speaker_2',), ('speaker_3',)  ]


        query_result_mock = Mock()
        query_result_mock.fetchall.return_value = speakers

        self.mock_data_processor.connection.execute.return_value = query_result_mock

        speaker_name_list = ','.join(
            [f"'{self.data_marker.escape_sql_special_char(speaker_name[0])}'" for speaker_name in speakers])
        expected_output = f'({speaker_name_list})'

        expected_param_dict = {
            "duration": 30,
            "speaker_count": 10,
        }
        actual_output = self.data_marker._get_speaker_name_list(speaker_criteria)
        self.assertEqual(expected_output, actual_output)

        curr_args = self.mock_data_processor.connection.execute.call_args_list

        self.assertEqual(curr_args[0][0][0].text, constants.SELECT_SPEAKER_FOR_DATA_GREATER_THAN_DURATION_QUERY)
        self.assertDictEqual(curr_args[0][1], expected_param_dict)
        self.assertEqual(len(curr_args), 1)
        self.assertEqual(self.mock_data_processor.connection.execute.call_count, 1)
        self.assertEqual(query_result_mock.fetchall.call_count, 1)




