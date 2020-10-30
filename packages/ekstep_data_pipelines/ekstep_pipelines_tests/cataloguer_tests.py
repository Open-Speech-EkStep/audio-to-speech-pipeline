import sys
import unittest
from audio_cataloguer.cataloguer import INSERT_UNIQUE_SPEAKER_QUERY
from unittest.mock import Mock
from unittest import mock



from audio_cataloguer.cataloguer import AudioCataloguer
sys.path.insert(0, '..')

class AudioCataloguerTests(unittest.TestCase):

    def setUp(self):
        self.postgres_client = Mock()
        self.cataloguer = AudioCataloguer(self.postgres_client)

    def test__get_utterance_list_should_call_execuete_query_return_utterence_list(self):
        dummy_audio_id = ['1234']
        self.postgres_client.execute_query.return_value = [['["1234"]']]

        actual_value = self.cataloguer.get_utterance_list(dummy_audio_id)

        self.assertEqual(self.postgres_client.execute_query.call_count,1)
        self.postgres_client.execute_query.assert_called_with('select utterances_files_list from media where audio_id = :audio_id', audio_id='1234')
        self.assertEqual(actual_value,['1234'])


    def test__parse_raw_file_data_should_convert_string_to_json(self):
        raw_data = '["12345"]'

        actual_value = self.cataloguer.parse_raw_file_data(raw_data)

        self.assertEqual(actual_value,["12345"])

    def test__create_insert_query_should_return_query_for_given_utterance(self):
        defult_query = "dummy_query"
        audio_id = ["121"]
        speaker_id = "dummy_speaker_id"
        datetime="10/10/20"
        utterance = {"name":"test_file1","duration":1.1,"snr_value":16.1,"status":"Clean"}


        actual_output = self.cataloguer.create_insert_query(utterance,speaker_id,audio_id,datetime,defult_query)

        expected_output = "dummy_query (121,dummy_speaker_id,'test_file1',1.1,'10/10/20',16.1,'Clean','','null'),"

        self.assertEqual(actual_output,expected_output)

    def test__create_insert_query_when_snr_value_is_nan_should_return_query_for_given_utterance(self):
        defult_query = "dummy_query"
        audio_id = ["121"]
        speaker_id = "dummy_speaker_id"
        datetime="10/10/20"
        utterance = {"name":"test_file1","duration":1.1,"snr_value":'nan',"status":"Clean"}

        actual_output = self.cataloguer.create_insert_query(utterance,speaker_id,audio_id,datetime,defult_query)

        expected_output = "dummy_query (121,dummy_speaker_id,'test_file1',1.1,'10/10/20',0.0,'Clean','','null'),"

        self.assertEqual(actual_output,expected_output)

    def test__get_load_date_time_should_return_expeted_value(self):

        dummy_audio_id = ['1234']
        self.postgres_client.execute_query.return_value = [["dummy_date"]]

        actual_value = self.cataloguer.get_load_datetime(dummy_audio_id)

        self.assertEqual(self.postgres_client.execute_query.call_count,1)
        self.postgres_client.execute_query.assert_called_with('select load_datetime from media where audio_id = :audio_id;', audio_id='1234')
        self.assertEqual(actual_value,'dummy_date')

    def test__set_isnormalized_flag_should_set_isnormalized_flag_to_true(self):
        dummy_audio_ids = ['123','1234','12345','12346']

        self.cataloguer.set_isnormalized_flag(dummy_audio_ids)

        self.assertEqual(self.postgres_client.execute_update.call_count,1)
        self.postgres_client.execute_update.assert_called_with('update media_metadata_staging set is_normalized = true where audio_id in (123,1234,12345,12346)')

    def test__set_isnormalized_flag_should_set_isnormalized_flag_to_true_when_audio_ids_is_nested(self):
        dummy_audio_ids = [['123'],['1234'],['12345'],['12346']]

        self.cataloguer.set_isnormalized_flag(dummy_audio_ids,True)

        self.assertEqual(self.postgres_client.execute_update.call_count,1)
        self.postgres_client.execute_update.assert_called_with('update media_metadata_staging set is_normalized = true where audio_id in (123,1234,12345,12346)')

    def test__find_speaker_id_should_return_speaker_id_of_given_audio_id(self):

        dummy_audio_id = ['1234']
        self.postgres_client.execute_query.return_value = [["12"]]

        actual_output= self.cataloguer.find_speaker_id(dummy_audio_id)

        self.assertEqual(self.postgres_client.execute_query.call_count,1)
        self.assertEqual(actual_output,'12')
        self.postgres_client.execute_query.assert_called_with('select speaker_id from speaker s JOIN media_metadata_staging b on s.speaker_name = b.speaker_name where b.audio_id = :audio_id;', audio_id='1234')

    def test__copy_data_from_media_metadata_staging_to_speaker_should_copy_data_from_staging_to_speaker_table(self):

        self.cataloguer.copy_data_from_media_metadata_staging_to_speaker()

        self.assertEqual(self.postgres_client.execute_update.call_count,1)
        self.postgres_client.execute_update.assert_called_with(INSERT_UNIQUE_SPEAKER_QUERY)
        

        




