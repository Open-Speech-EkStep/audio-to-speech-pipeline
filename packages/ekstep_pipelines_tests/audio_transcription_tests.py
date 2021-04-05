import os
import unittest
from unittest.mock import Mock

from ekstep_data_pipelines.audio_transcription.audio_transcription import (
    AudioTranscription,
)
from ekstep_data_pipelines.audio_transcription.constants import AUDIO_LANGUAGE
from ekstep_data_pipelines.common.audio_commons.transcription_clients.transcription_client_errors \
    import (
    GoogleTranscriptionClientError, )


class AudioTranscriptionTests(unittest.TestCase):
    def setUp(self):
        self.postgres_client = Mock()
        self.gcp_instance = Mock()
        self.audio_commons = {"transcription_clients": Mock()}
        self.catalogue_dao = Mock()
        self.audio_transcription = AudioTranscription(
            self.postgres_client,
            self.gcp_instance,
            self.audio_commons,
            self.catalogue_dao,
        )
        self.audio_transcription.fs_interface = Mock()

    def test__delete_audio_id_should_call_gcp_delete_object_method(self):
        self.audio_transcription.delete_audio_id(
            "remote_dir_path_for_given_audio_id")

        self.assertEqual(
            self.audio_transcription.fs_interface.delete.call_count, 1)

    def test__get_local_dir_path_should_return_dir_path(self):
        actual_output = self.audio_transcription.get_local_dir_path(
            "testdir/inside_dir/test_local_file_path/test_filename.txt"
        )

        self.assertEqual(
            actual_output,
            "testdir/inside_dir/test_local_file_path")

    @unittest.mock.patch("os.system")
    def test__handle_error_should_call_update_db_and_move_rejected_data_to_rejected_folder_create_rejected_folder_if_not_exist(
            self, mock_os
    ):
        metadata = {"status": "test_status", "reason": "test_reason"}

        self.audio_transcription.handle_error(
            1234,
            "testdir/local_clean_path",
            "testdir/local_rejected_path",
            metadata,
            "reason_of_fail",
        )

        self.assertEqual(
            self.catalogue_dao.update_utterance_status.call_count, 1)
        self.catalogue_dao.update_utterance_status.assert_called_with(
            1234, metadata)
        self.assertEqual(mock_os.call_count, 1)
        mock_os.assert_called_with(
            "mv testdir/local_clean_path testdir/local_rejected_path"
        )

    def test__generate_transcription_and_sanitize_called_with_filename_that_is_not_contained_wav_extension_shoud_not_call_any_function(
            self,
    ):
        file_path = "filename_without_extension"

        transcription_client = self.audio_commons.get("transcription_clients")

        metadata = {"status": "test_status", "reason": "test_reason"}

        self.audio_transcription.generate_transcription_and_sanitize(
            1234,
            "testdir/local_clean_path",
            "testdir/local_rejected_path",
            file_path,
            "language",
            transcription_client,
            metadata,
        )

        self.assertEqual(
            transcription_client.generate_transcription.call_count, 0)

    def test__generate_transcription_and_sanitize_called_with_filename_that_contained_wav_extension_shoud_call_generate_transcription(
            self,
    ):
        file_name = "filename_with_extension.wav"

        transcription_client = self.audio_commons.get("transcription_clients")
        transcription_client.generate_transcription.return_value = "अलग अलग होते है"

        self.audio_transcription.audio_transcription_config = {
            AUDIO_LANGUAGE: "hindi"}
        metadata = {"status": "test_status", "reason": "test_reason"}

        self.audio_transcription.generate_transcription_and_sanitize(
            1234,
            "local_clean_file.wav",
            "testdir/local_rejected_path",
            file_name,
            "language",
            transcription_client,
            metadata,
        )

        self.assertEqual(
            self.audio_transcription.fs_interface.download_file_to_location.call_count,
            1,
        )
        self.audio_transcription.fs_interface.download_file_to_location.assert_called_with(
            file_name, "local_clean_file.wav")

        self.assertEqual(
            transcription_client.generate_transcription.call_count, 1)
        transcription_client.generate_transcription.assert_called_with(
            "language", "local_clean_file.wav"
        )

        self.assertTrue(os.path.exists("local_clean_file.txt"))

    def test__generate_transcription_and_sanitize_called_with_filename_that_contained_wav_extension_when_generate_transcription_throw_error(
            self,
    ):
        # file_path = Mock()

        file_path = "filename_with_extension.wav"

        transcription_client = self.audio_commons.get("transcription_clients")
        transcription_client.generate_transcription.side_effect = (
            GoogleTranscriptionClientError("test_google_error")
        )

        metadata = {"status": "test_status", "reason": "test_reason"}

        self.audio_transcription.generate_transcription_and_sanitize(
            1234,
            "testdir/local_clean_path/local_clean_file.wav",
            "testdir/local_rejected_path",
            file_path,
            "language",
            transcription_client,
            metadata,
        )

        self.assertEqual(
            self.audio_transcription.fs_interface.download_file_to_location.call_count,
            1,
        )
        self.audio_transcription.fs_interface.download_file_to_location.assert_called_with(
            file_path, "testdir/local_clean_path/local_clean_file.wav")

        self.assertEqual(
            transcription_client.generate_transcription.call_count, 1)
        transcription_client.generate_transcription.assert_called_with(
            "language", "testdir/local_clean_path/local_clean_file.wav"
        )

        self.assertEqual(
            self.catalogue_dao.update_utterance_status.call_count, 1)
        self.catalogue_dao.update_utterance_status.assert_called_with(
            1234, metadata)

    def test__generate_transcription_for_all_utterenaces_should_do_transcription_for_all_file_in_given_audio_id_when_should_skip_rejected_is_false(
            self,
    ):
        file_one_path = "testdir/file_one_with_extension.wav"
        file_two_path = "testdir/file_two_with_extension.wav"
        file_three_path = "testdir/file_three_with_extension.wav"

        list_of_file_path = [file_one_path, file_two_path, file_three_path]

        transcription_client = self.audio_commons.get("transcription_clients")

        self.catalogue_dao.find_utterance_by_name.side_effect = [
            {"status": "Clean", "reason": "test_reason", "duration": 3},
            {"status": "redacted", "reason": "test_reason", "duration": 3},
            {"status": "Clean", "reason": "test_reason", "duration": 3},
        ]

        self.audio_transcription.generate_transcription_for_all_utterenaces(
            12343,
            list_of_file_path,
            "language",
            transcription_client,
            "utterenaces",
            False,
            "remote_path",
        )

        self.assertEqual(
            self.catalogue_dao.find_utterance_by_name.call_count, 3)

        self.assertEqual(
            transcription_client.generate_transcription.call_count, 3)

    def test__generate_transcription_for_all_utterenaces_should_do_transcription_for_only_clean_file_in_given_audio_id_when_should_skip_rejected_is_true(
            self,
    ):
        file_one_path = "testdir/file_one_with_extension.wav"
        file_two_path = "testdir/file_two_with_extension.wav"
        file_three_path = "testdir/file_three_with_extension.wav"

        list_of_file_path = [file_one_path, file_two_path, file_three_path]

        transcription_client = self.audio_commons.get("transcription_clients")

        self.catalogue_dao.find_utterance_by_name.side_effect = [
            {"status": "Clean", "reason": "test_reason", "duration": 3},
            {"status": "Rejected", "reason": "test_reason", "duration": 4},
            {"status": "Clean", "reason": "test_reason", "duration": 5},
        ]

        self.audio_transcription.generate_transcription_for_all_utterenaces(
            12343,
            list_of_file_path,
            "language",
            transcription_client,
            "utterenaces",
            True,
            "remote_path",
        )

        self.assertEqual(
            self.catalogue_dao.find_utterance_by_name.call_count, 3)

        self.assertEqual(
            transcription_client.generate_transcription.call_count, 2)

    def test__generate_transcription_for_all_utterenaces_should_do_transcription_for_only_clean_file_and_duration_is_in_threshold_and_in_given_audio_id_when_should_skip_rejected_is_true(
            self,
    ):
        file_one_path = "testdir/file_one_with_extension.wav"
        file_two_path = "testdir/file_two_with_extension.wav"
        file_three_path = "testdir/file_three_with_extension.wav"
        file_four_path = "testdir/file_four_with_extension.wav"

        list_of_file_path = [
            file_one_path,
            file_two_path,
            file_three_path,
            file_four_path,
        ]

        transcription_client = self.audio_commons.get("transcription_clients")

        self.catalogue_dao.find_utterance_by_name.side_effect = [
            {"status": "Clean", "reason": "test_reason", "duration": 0.2},
            {"status": "Rejected", "reason": "test_reason", "duration": 3},
            {"status": "Clean", "reason": "test_reason", "duration": 18},
            {"status": "Clean", "reason": "test_reason", "duration": 13},
        ]

        self.audio_transcription.generate_transcription_for_all_utterenaces(
            12343,
            list_of_file_path,
            "language",
            transcription_client,
            "utterenaces",
            True,
            "remote_path",
        )

        self.assertEqual(
            self.catalogue_dao.find_utterance_by_name.call_count, 4)

        self.assertEqual(
            transcription_client.generate_transcription.call_count, 1)
