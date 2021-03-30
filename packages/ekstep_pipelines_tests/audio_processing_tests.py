import sys
import unittest
from ekstep_data_pipelines.audio_processing import constants
from unittest.mock import Mock
from unittest import mock


from ekstep_data_pipelines.audio_processing import generate_hash

from ekstep_data_pipelines.audio_processing.audio_processer import AudioProcessor


class AudioProcessorTests(unittest.TestCase):
    def setUp(self):
        self.postgres_client = Mock()
        self.gcp_instance = Mock()
        self.catalogue_dao = Mock()

        self.audio_commons = {
            "snr_util": Mock(),
            "chunking_conversion": Mock()}
        self.audio_processer = AudioProcessor(
            self.postgres_client,
            self.gcp_instance,
            self.audio_commons,
            self.catalogue_dao,
        )

        self.audio_processer.fs_interface = Mock()

    def test__should_call_convert_to_wav_and_return_path_when_file_converted(
            self):
        self.audio_commons["chunking_conversion"].convert_to_wav.return_value = (
            "test_output_path", True, )

        actual_output = self.audio_processer._convert_to_wav("testdir", "mp4")

        self.assertEqual(actual_output, "test_output_path")
        self.assertEqual(
            self.audio_commons["chunking_conversion"].convert_to_wav.call_count, 1)

    def test__should_call_convert_to_wav_and_return_none_when_file_not_converted(
            self):
        self.audio_commons["chunking_conversion"].convert_to_wav.return_value = (
            "test_output_path", None, )

        actual_output = self.audio_processer._convert_to_wav("testdir", "mp4")

        self.assertEqual(actual_output, None)
        self.assertEqual(
            self.audio_commons["chunking_conversion"].convert_to_wav.call_count, 1)

    def test__should__call_break_files_into_chunks_and_return_chanks_dir_path(
            self):
        self.audio_processer.audio_processor_config = {
            constants.CHUNKING_CONFIG: {
                "aggressiveness": 2, "max_duration": 13}}

        actual_output = self.audio_processer._break_files_into_chunks(
            12345, "test_local_download_path", "test_input_wav_file"
        )

        self.assertEqual(
            self.audio_commons["chunking_conversion"].create_audio_clips.call_count, 1)
        self.assertEqual(actual_output, "test_local_download_path/chunks")

    def test__should__call_break_files_into_chunks_and_throw_excepction_when_aggressiveness_is_not_a_int(
        self,
    ):
        self.audio_processer.audio_processor_config = {
            constants.CHUNKING_CONFIG: {
                "aggressiveness": "2", "max_duration": 13}}

        self.assertRaises(
            Exception,
            self.audio_processer._break_files_into_chunks,
            12345,
            "test_local_download_path",
            "test_input_wav_file",
        )
        self.assertEqual(
            self.audio_commons["chunking_conversion"].create_audio_clips.call_count, 0)

    def test__should_call_move_of_fs_interfase_when_move_file_to_done_folder_called(
        self,
    ):

        self.audio_processer.audio_processor_config = {
            constants.SNR_DONE_FOLDER_PATH: "dummy_path"
        }

        self.audio_processer.move_file_to_done_folder(
            "audio_file_path/testdir",
            "meta_data_file_path",
            "test_source",
            "file_name",
            "meta_data_file",
        )

        self.assertEqual(self.audio_processer.fs_interface.move.call_count, 2)

    @mock.patch(
        "ekstep_data_pipelines.audio_processing.audio_processer.get_hash_code_of_audio_file"
    )
    def test__process_audio_id_when_file_is_duplicate_should_not_call_SNR(
        self, generate_hash_mock
    ):

        self.catalogue_dao.check_file_exist_in_db.return_value = True

        generate_hash_mock.return_value = "12334"

        self.audio_processer.audio_processor_config = {
            constants.REMOTE_RAW_FILE: "dummy_path",
            constants.REMOTE_PROCESSED_FILE_PATH: "asfbka",
            constants.SNR_DONE_FOLDER_PATH: "somepath",
        }

        self.audio_processer.process_audio_id(
            "audio_id", "source", "extension", "file_name"
        )
        self.assertEqual(self.audio_processer.fs_interface.move.call_count, 2)
        self.assertEqual(
            self.audio_processer.fs_interface.download_file_to_location.call_count, 2)
        self.assertEqual(
            self.audio_commons["chunking_conversion"].convert_to_wav.call_count, 0)
        self.assertEqual(
            self.audio_processer.fs_interface.upload_folder_to_location.call_count, 0)

        self.assertEqual(
            self.catalogue_dao.check_file_exist_in_db.call_count, 1)
        self.assertEqual(self.catalogue_dao.upload_file.call_count, 0)

        self.catalogue_dao.check_file_exist_in_db.assert_called_with(
            "file_name", "12334"
        )

    @mock.patch(
        "ekstep_data_pipelines.audio_processing.audio_processer.get_hash_code_of_audio_file"
    )
    def test__process_audio_id_when_file_is_not_duplicate_should_call_SNR(
        self, generate_hash_mock
    ):

        self.catalogue_dao.check_file_exist_in_db.return_value = False
        generate_hash_mock.return_value = "12334"
        self.audio_commons["chunking_conversion"].convert_to_wav.return_value = (
            "output_file_path", True, )

        snr_config = {"max_snr_threshold": 15}

        self.audio_processer.audio_processor_config = {
            constants.REMOTE_RAW_FILE: "dummy_path",
            constants.REMOTE_PROCESSED_FILE_PATH: "processed_path",
            constants.SNR_DONE_FOLDER_PATH: "somepath",
            constants.SNR_CONFIG: snr_config,
        }

        self.audio_processer.process_audio_id(
            "audio_id", "source", "mp4", "file_name.mp4"
        )
        self.assertEqual(self.audio_processer.fs_interface.move.call_count, 2)
        self.assertEqual(
            self.audio_processer.fs_interface.download_file_to_location.call_count, 2)
        self.assertEqual(
            self.audio_processer.fs_interface.upload_folder_to_location.call_count, 2)
        self.assertEqual(
            self.audio_commons["chunking_conversion"].convert_to_wav.call_count, 1)
        self.assertEqual(self.catalogue_dao.upload_file.call_count, 1)

        self.assertEqual(
            self.catalogue_dao.check_file_exist_in_db.call_count, 1)
        self.catalogue_dao.check_file_exist_in_db.assert_called_with(
            "file_name.mp4", "12334"
        )
