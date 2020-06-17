import unittest
import sqlite3
# from sqlalchemy import  text
from src.scripts.db_normalizer import Db_normalizer
from unittest import mock
from unittest.mock import MagicMock, patch, Mock
from src.tests.tests_resources.initialize_db import create_db,find_count

class TestDbNormalizer(unittest.TestCase):

    def setUp(self):
        super(TestDbNormalizer, self).setUp()
        self.normalizer = Db_normalizer()
        self.conn = sqlite3.connect(':memory:')

        self.c = self.conn.cursor()
        create_db(self.c,self.conn)

    def test_copy_data_from_media_metadata_staging_to_speaker_should_return_expected_value(self):
        self.normalizer.copy_data_from_media_metadata_staging_to_speaker(self.conn)
        self.assertEqual(find_count(self.conn,'speaker'),3 )

    def test_insert_file_should_return_equal_row(self):
        self.normalizer.insert_file(self.conn,'src/tests/tests_resources/query.txt')
        self.assertEqual(find_count(self.conn,'media_speaker_mapping'), 2)

    def test_get_max_load_datetime_should_return_max_load_datetime(self):
        self.normalizer.insert_file(self.conn,'src/tests/tests_resources/query.txt')
        result = self.normalizer.get_load_date_for_mapping(self.conn)
        self.assertEqual(result, '123445')