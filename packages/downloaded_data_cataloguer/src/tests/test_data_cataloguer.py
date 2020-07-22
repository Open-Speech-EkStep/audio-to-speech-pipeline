import unittest
# import sqlite3
from src.scripts.data_cataloguer import CatalogueDownloadedData, get_file_extension, get_files_path_with_no_prefix
from src.scripts.gcs_operations import CloudStorageOperations


from unittest import mock
# from unittest.mock import MagicMock, patch, Mock


# def mock_move_blob(*args, **kwargs):
#     print("hello")
#     return True


class TestDataCataloguer(unittest.TestCase):

    def setUp(self):
        pass
        # super(TestDataCataloguer, self).setUp()
        self.cataloguer = CatalogueDownloadedData()

    def test_assert_true(self):
        self.assertTrue(True)

    def test_assert_false(self):
        self.assertFalse(False)

    def test_get_file_extension_of_file_with_no_extension(self):
        assert_expected = get_file_extension(file_name="audio_file")
        self.assertEqual("audio_file", assert_expected)

    def test_get_file_extension_of_mp3_file(self):
        assert_expected = get_file_extension(file_name="audio_file.mp3")
        self.assertEqual("mp3", assert_expected)

    def test_get_file_extension_of_csv_file(self):
        assert_expected = get_file_extension(file_name="audio_file.csv")
        self.assertEqual("csv", assert_expected)

    def test_get_files_path_with_no_prefix(self):
        assert_expected = get_files_path_with_no_prefix(
            file_path="data/audiotospeech/raw/download/hindi/audio/mahatmagandhi", file_name="audio_file_1")
        self.assertEqual("data/audiotospeech/raw/download/hindi/audio/mahatmagandhi/audio_file_1", assert_expected)

    def test_file_has_mp3_extension_returns_true(self):
        assert_expected = self.cataloguer.has_mp3_extension(expected_file_extension='mp3', file_extension='MP3')
        self.assertTrue(True, assert_expected)

    def test_get_file_name(self):
        assert_expected = self.cataloguer.get_file_name(file_prefix_name="raw/data/audio_file.mp3", delimiter="/")
        self.assertEqual("audio_file.mp3", assert_expected)

    def test_get_metadata_file_name(self):
        assert_expected = self.cataloguer.get_metadata_file_name(file_name="audio_file.mp3")
        self.assertEqual("audio_file.csv", assert_expected)

    def test_condition_file_name(self):
        assert_expected = self.cataloguer.condition_file_name(file_name="audio_file(1).csv")
        self.assertEqual("audio_file1.csv", assert_expected)
    # def test_move_blob(self):
    #     with mock.patch.object(CloudStorageOperations, 'move_blob', new=mock_move_blob):
    #         obj_gcs_ops = CloudStorageOperations()
    #         # print(obj_gcs_ops.move_blob())
    #         # self.cataloguer.move_to_error('test','abc.mp3','abc.csv','test','abc')
    #         #print(MockCatalogueDownloadedData)
    #         self.assertEqual(" ", self.cataloguer.move_to_error('test','abc.mp3','abc.csv','test','abc'))

    # def test_move_to_error(self):
    #     self.cataloguer.move_to_error(error_landing_path=" ", file_name=" ", metadata_file_name=" ", obj_gcs_ops=" ", source=" ", source_file_name=" ")
    #     pass

    # def test_copy_data_from_media_metadata_staging_to_speaker_should_return_expected_value(self):
    #     self.cataloguer.copy_data_from_media_metadata_staging_to_speaker(self.conn)
    #     self.assertEqual(find_count(self.conn, 'speaker'), 3)
    #
    # def test_insert_file_should_return_equal_row(self):
    #     self.cataloguer.insert_file(self.conn, 'src/tests/tests_resources/query.txt')
    #     self.assertEqual(find_count(self.conn, 'media_speaker_mapping'), 2)
    #
    # def test_get_max_load_datetime_should_return_max_load_datetime(self):
    #     self.cataloguer.insert_file(self.conn, 'src/tests/tests_resources/query.txt')
    #     result = self.cataloguer.get_load_date_for_mapping(self.conn)
    #     self.assertEqual(result, '123445')
