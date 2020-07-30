import unittest
import sqlite3
from unittest import mock
from src.scripts.data_tagger import ExperimentDataTagger, validate_input,validate_existing_exp_input

from src.tests.tests_resources.initialize_db import create_db, find_used_speaker_count, find_count


class TestExperimentDataTagger(unittest.TestCase):

    def setUp(self):
        super(TestExperimentDataTagger, self).setUp()
        self.tagger = ExperimentDataTagger()
        # self.data_obj = Data()
        self.conn = sqlite3.connect(':memory:')

        self.c = self.conn.cursor()
        create_db(self.c, self.conn)

    def test_update_table_should_return_expected_value(self):
        self.tagger.update_table(
            self.conn, "update media_speaker_mapping set speaker_exp_use_status = 'true' WHERE speaker_id IN (5,6,")
        self.assertEqual(find_used_speaker_count(self.c), 2)

    def test_insert_into_media_speaker_mapping_should_insert_into_media_speaker_mapping(self):
        self.tagger.insert_into_media_speaker_mapping(self.conn, "insert into media_speaker_mapping(audio_id, speaker_id, clipped_utterance_file_name, clipped_utterance_duration,\
        load_datetime,experiment_id,experiment_use_status,speaker_exp_use_status) values (234,32,'abc.wav',3,2342,111,'true','true'),(234,32,'abc.wav',3,2342,111,'true','true'),")
        self.assertEqual(find_count(self.c, 'media_speaker_mapping'), 4)

    def test_clean_duration_threshold_should_return_right_query(self):
        result = self.tagger.clean_duration_threshold(
            [(123, 'file_name', 1), (123, 'file_name1', 2), (123, 'file_name2', 1)], 3, 3)
        self.assertEqual(
            result, "update media_speaker_mapping_test_with_yaml set experiment_use_status = true,experiment_id= 3 WHERE clipped_utterance_file_name IN ( 'file_name','file_name1',")

    def test_validate_input_should_not_raised_exception_for_valid_input(self):
        raised = False
        try:
            validate_input(4,5,'exp10',False)
        except:
            raised = True
        self.assertFalse(raised, 'Exception not raised')

    def test_validate_input_should_throw_exception_if_input_value_is_zero(self):
        self.assertRaises(ValueError,validate_input,0,0,'exp10',False)

    def test_validate_input_should_throw_exception_if_num_speaker_is_zero(self):
        self.assertRaises(ValueError,validate_input,0,5,'exp10',False)

    def test_validate_input_should_throw_exception_input_type_is_not_int(self):
            self.assertRaises(ValueError,validate_input,' ',' ','exp10',False)

    def test_validate_input_should_raised_exception_if_exp_name_is_null(self):
        self.assertRaises(ValueError,validate_input,2,3,'   ',False)

    def test_validate_existing_exp_input_should_not_raised_exception_for_existing_exp_is_False(self):
        raised = False
        try:
            validate_existing_exp_input(False,0,'  ',False)
        except:
            raised = True
        self.assertFalse(raised, 'Exception not raised')

    def test_validate_existing_exp_input_should_throw_exception_if_num_speaker_is_zero_and_existing_exp_is_true(self):
        self.assertRaises(ValueError,validate_existing_exp_input,True,0,'exp10',False)

    def test_validate_existing_exp_input_should_throw_exception_if_exp_name_is_empty_and_existing_exp_is_true(self):
            self.assertRaises(ValueError,validate_existing_exp_input,True,6,'   ',False)
