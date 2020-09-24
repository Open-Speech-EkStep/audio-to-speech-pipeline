import sys
import unittest
import os
from audio_processing import constants
from unittest.mock import Mock


from audio_transcription.audio_transcription import AudioTranscription
sys.path.insert(0, '..')


class AudioTranscriptionTests(unittest.TestCase):

    def setUp(self):
        self.postgres_client = Mock()
        self.gcp_instance = Mock()
        self.audio_commons = {"snr_util": Mock(), "chunking_conversion": Mock()}
        self.catalogue_dao = Mock()
        self.audio_transcription = AudioTranscription(self.postgres_client, self.gcp_instance,self.audio_commons,self.catalogue_dao)

    def test__delete_audio_id_should_call_gcp_delete_object_method(self):

        self.audio_transcription.delete_audio_id('remote_dir_path_for_given_audio_id')

        self.assertEqual(self.gcp_instance.delete_object.call_count,1)

    def test__move_to_gcs_should_call_gcp_upload_to_gcs_method(self):

        self.audio_transcription.move_to_gcs('local_path', 'remote_stt_output_path')

        self.assertEqual(self.gcp_instance.upload_to_gcs.call_count,1)

    def test__get_local_dir_path_should_return_dir_path(self):

        actual_output = self.audio_transcription.get_local_dir_path('testdir/inside_dir/test_local_file_path/test_filename.txt')

        self.assertEqual(actual_output,'testdir/inside_dir/test_local_file_path')

    @unittest.mock.patch('os.system')
    def test__handle_error_should_call_update_db_and_move_rejected_data_to_rejected_folder_create_rejected_folder_if_not_exist(self,mock_os):

        metadata = {"status":"test_status","reason":"test_reason"}

        self.audio_transcription.handle_error(1234,'testdir/local_clean_path','testdir/local_rejected_path',metadata,"reason_of_fail")

        self.assertEqual(self.catalogue_dao.update_utterance_status.call_count,1)
        self.catalogue_dao.update_utterance_status.assert_called_with(1234,metadata)
        self.assertEqual(mock_os.call_count,1)
        mock_os.assert_called_with('mv testdir/local_clean_path testdir/local_rejected_path')



    

    