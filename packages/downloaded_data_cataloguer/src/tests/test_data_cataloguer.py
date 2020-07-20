import unittest
import sqlite3
from ..scripts.data_cataloguer import CatalogueDownloadedData
from unittest import mock
from unittest.mock import MagicMock, patch, Mock


class TestDataCataloguer(unittest.TestCase):

    def setUp(self):
        super(TestDataCataloguer, self).setUp()
        self.cataloguer = CatalogueDownloadedData()

    def test_copy_data_from_media_metadata_staging_to_speaker_should_return_expected_value(self):
        self.cataloguer.copy_data_from_media_metadata_staging_to_speaker(self.conn)
        self.assertEqual(find_count(self.conn, 'speaker'), 3)

    def test_insert_file_should_return_equal_row(self):
        self.cataloguer.insert_file(self.conn, 'src/tests/tests_resources/query.txt')
        self.assertEqual(find_count(self.conn, 'media_speaker_mapping'), 2)

    def test_get_max_load_datetime_should_return_max_load_datetime(self):
        self.cataloguer.insert_file(self.conn, 'src/tests/tests_resources/query.txt')
        result = self.cataloguer.get_load_date_for_mapping(self.conn)
        self.assertEqual(result, '123445')
