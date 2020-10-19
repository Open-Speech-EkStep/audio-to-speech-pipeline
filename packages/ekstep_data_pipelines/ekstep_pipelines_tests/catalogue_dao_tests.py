import json
import sys
import unittest
from unittest import mock

from common import PostgresClient
from common.dao.catalogue_dao import CatalogueDao

sys.path.insert(0, '..')


class CatalogueTests(unittest.TestCase):

    @mock.patch('common.postgres_db_client.PostgresClient')
    def test_get_utterances(self, mock_postgres_client):
        mock_postgres_client.execute_query.return_value = \
            [(
             '[{"name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav", "duration": "13.38", "snr_value": 38.432806, "status": "Clean"}]',)]
        catalogueDao = CatalogueDao(mock_postgres_client)
        audio_id = '2020'
        actual_utterances = catalogueDao.get_utterances(audio_id)
        expected_utterances = \
            [{"name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav", "duration": "13.38", "snr_value": 38.432806,
              "status": "Clean"}]
        self.assertEqual(expected_utterances, actual_utterances)

    @mock.patch('common.postgres_db_client.PostgresClient')
    def test_get_utterances_as_empty_if_no_records(self, mock_postgres_client):
        mock_postgres_client.execute_query.return_value = []
        catalogueDao = CatalogueDao(mock_postgres_client)
        audio_id = '2020'
        actual_utterances = catalogueDao.get_utterances(audio_id)
        expected_utterances = []
        self.assertEqual(expected_utterances, actual_utterances)

    @mock.patch('common.postgres_db_client.PostgresClient')
    def test_update_utterances(self, mock_postgres_client):
        mock_postgres_client.execute_update.return_value = None
        catalogueDao = CatalogueDao(mock_postgres_client)
        audio_id = '2020'
        utterances = [{'name': '190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav',
                       'duration': '13.38', 'snr_value': 38.432806, 'status': 'Clean'}]

        rows_updated = catalogueDao.update_utterances(audio_id, utterances)
        args = mock_postgres_client.execute_update.call_args_list
        called_with_params = {"utterances": json.dumps(utterances),
                              "audio_id": audio_id}
        called_with_sql = 'update media_metadata_staging set utterances_files_list = :utterances where audio_id = :audio_id'
        self.assertEqual(rows_updated, True)
        self.assertEqual(called_with_sql, args[0][0][0])
        self.assertEqual(called_with_params, args[0][1])

    @mock.patch('common.postgres_db_client.PostgresClient')
    def test_utterance_by_name(self, mock_postgres_client):
        catalogueDao = CatalogueDao(mock_postgres_client)
        name = '190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav'
        utterances = [
            {"name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav", "duration": "13.38", "snr_value": 38.432806,
             "status": "Clean"},
            {"name": "91_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav", "duration": "3.27", "snr_value": 37.0,
             "status": "Clean"}]
        utterance = catalogueDao.find_utterance_by_name(utterances, name)
        self.assertEqual(
            {"name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav", "duration": "13.38", "snr_value": 38.432806,
             "status": "Clean"}
            , utterance)

    @mock.patch('common.postgres_db_client.PostgresClient')
    def test_utterance_by_name_return_None_if_not_found(self, mock_postgres_client):
        catalogueDao = CatalogueDao(mock_postgres_client)
        name = 'not_exists.wav'
        utterances = [
            {"name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav", "duration": "13.38", "snr_value": 38.432806,
             "status": "Clean"},
            {"name": "91_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav", "duration": "3.27", "snr_value": 37.0,
             "status": "Clean"}]

        utterance = catalogueDao.find_utterance_by_name(utterances, name)
        self.assertEqual(None, utterance)

    @mock.patch('common.postgres_db_client.PostgresClient')
    def test_update_utterance_status(self, mock_postgres_client):
        catalogueDao = CatalogueDao(mock_postgres_client)
        audio_id = '2020'
        utterance = {"name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav",
                     "duration": "13.38", "snr_value": 38.432806, "status": "Clean", 'reason': 'stt error'}

        rows_updated = catalogueDao.update_utterance_status(audio_id, utterance)

        args = mock_postgres_client.execute_update.call_args_list
        called_with_query = 'update media_speaker_mapping set status = :status, ' \
                            'fail_reason = :reason where audio_id = :audio_id ' \
                            'and clipped_utterance_file_name = :name'

        called_with_args = {'status': 'Clean', 'reason': 'stt error', 'audio_id': audio_id,
                            'name': '190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav'}
        self.assertEqual(True, rows_updated)
        self.assertEqual(called_with_query, args[0][0][0])
        self.assertEqual(called_with_args, args[0][1])

    @mock.patch('common.postgres_db_client.PostgresClient')
    def test_get_utterances_by_source(self, mock_postgres_client):
        source = 'test_source'
        status = 'Clean'
        # speaker_id, clipped_utterance_file_name, clipped_utterance_duration, audio_id, snr
        expected_utterances = [
            (1, 'file_1.wav', '10', '2010123', 16),
            (2, 'file_2.wav', '11', '2010124', 17),
            (3, 'file_3.wav', '12', '2010125', 18),
            (4, 'file_4.wav', '13', '2010126', 19)
        ]
        called_with_sql = 'select speaker_id, clipped_utterance_file_name, clipped_utterance_duration, audio_id, snr ' \
                          'from media_speaker_mapping ' \
                          'where audio_id in ' \
                          '(select audio_id from media_metadata_staging ' \
                          'where "source" = :source) ' \
                          'and status = :status ' \
                          'and staged_for_transcription = false ' \
                          'and clipped_utterance_duration >= 0.5 and clipped_utterance_duration <= 15'
        mock_postgres_client.execute_query.return_value = expected_utterances
        catalogueDao = CatalogueDao(mock_postgres_client)
        args = mock_postgres_client.execute_query.call_args_list
        utterances = catalogueDao.get_utterances_by_source(source, status)
        self.assertEqual(utterances, expected_utterances)
        self.assertEqual(called_with_sql, args[0][0][0])

    @mock.patch('common.postgres_db_client.PostgresClient')
    def test__should_update_utterance_staged_for_transcription(self, mock_postgres_client):
        mock_postgres_client.execute_batch.return_value = 2
        utterances = [
            (1, 'file_1.wav', '10', 2010123, 16),
            (2, 'file_2.wav', '11', 2010124, 16)
        ]
        catalogueDao = CatalogueDao(mock_postgres_client)
        catalogueDao.update_utterances_staged_for_transcription(utterances, 'test_source')
        called_with_query = 'update media_speaker_mapping set staged_for_transcription = true ' \
                            'where audio_id in (select audio_id from media_metadata_staging ' \
                            'where "source" = :source) and clipped_utterance_file_name in (\'file_1.wav\',\'file_2.wav\')'

        called_with_args = {"source": "test_source"}
        args = mock_postgres_client.execute_update.call_args
        self.assertEqual(called_with_query, args[0][0])
        self.assertEqual(called_with_args, args[1])

    @mock.patch('common.postgres_db_client.PostgresClient')
    def test__should_update_utterance_staged_for_transcription_empty_list(self, mock_postgres_client):
        mock_postgres_client.execute_update.return_value = 2
        utterances = []
        catalogueDao = CatalogueDao(mock_postgres_client)
        catalogueDao.update_utterances_staged_for_transcription(utterances, 'test_source')
        args = mock_postgres_client.execute_update.call_args
        self.assertEqual(None, args)

    @mock.patch('common.postgres_db_client.PostgresClient')
    def test__should_update_utterance_speaker(self, mock_postgres_client):
        mock_postgres_client.execute_update.return_value = 2
        utterances = [
            'file_1.wav',
            'file_2.wav'
        ]
        speaker_id = 10
        catalogueDao = CatalogueDao(mock_postgres_client)
        catalogueDao.update_utterance_speaker(utterances, speaker_id)
        called_with_query = 'update media_speaker_mapping set speaker_id=:speaker_id where clipped_utterance_file_name in (\'file_1.wav\',\'file_2.wav\')'
        called_with_args = {"speaker_id": speaker_id}
        args = mock_postgres_client.execute_update.call_args
        self.assertEqual(called_with_query, args[0][0])
        self.assertEqual(called_with_args, args[0][1])

    @mock.patch('common.postgres_db_client.PostgresClient')
    def test__should_insert_speakers(self, mock_postgres_client):
        mock_postgres_client.execute_update.return_value = 2
        catalogueDao = CatalogueDao(mock_postgres_client)
        catalogueDao.insert_speaker('test_source', 'test_speaker')
        called_with_query = 'insert into speaker (source, speaker_name) values (:source, :speaker_name)'
        called_with_args = {'source': 'test_source', 'speaker_name': 'test_speaker'}
        args = mock_postgres_client.execute_update.call_args
        self.assertEqual(called_with_query, args[0][0])
        self.assertEqual(called_with_args, args[1])