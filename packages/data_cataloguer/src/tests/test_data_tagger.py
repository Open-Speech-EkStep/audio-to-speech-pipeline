import unittest
import sqlite3
from unittest import mock
from src.scripts.data_tagger import ExperimentDataTagger

from src.tests.tests_resources.initialize_db import create_db,find_used_speaker_count,find_count

class TestExperimentDataTagger(unittest.TestCase):

    def setUp(self):
        super(TestExperimentDataTagger, self).setUp()
        self.tagger = ExperimentDataTagger()
        # self.data_obj = Data()
        self.conn = sqlite3.connect(':memory:')

        self.c = self.conn.cursor()
        create_db(self.c,self.conn)

    def test_update_table_should_return_expected_value(self):
        self.tagger.update_table(self.conn,"update media_speaker_mapping set speaker_exp_use_status = true WHERE speaker_id IN (5,6,")
        self.assertEqual(find_used_speaker_count(self.c),2)

    def test_insert_into_media_speaker_mapping_should_insert_into_media_speaker_mapping(self):
        self.tagger.insert_into_media_speaker_mapping(self.conn,"insert into media_speaker_mapping(audio_id, speaker_id, clipped_utterance_file_name, clipped_utterance_duration,\
        load_datetime,experiment_id,experiment_use_status,speaker_exp_use_status) values (234,32,'abc.wav',3,2342,111,'true','true'),(234,32,'abc.wav',3,2342,111,'true','true'),")
        self.assertEqual(find_count(self.c,'media_speaker_mapping'),4)