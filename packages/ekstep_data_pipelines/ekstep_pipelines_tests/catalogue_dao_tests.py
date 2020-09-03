import sys
import unittest
from unittest import mock

from common.dao.catalogue_dao import CatalogueDao

sys.path.insert(0, '..')

class CatalogueTests(unittest.TestCase):

    @mock.patch('common.postgres_db_client.PostgresClient')
    def test_get_utterances(self, mock_postgres_client):
        mock_postgres_client.execute_query.return_value = '[{"name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav", ' \
                              '"duration": "13.38", "snr_value": 38.432806, "status": "Clean"}'

        catalogueDao = CatalogueDao(mock_postgres_client)
        audio_id = '2020'
        actual_utterances = catalogueDao.get_utterances(audio_id)
        expected_utterances = '[{"name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav", ' \
                              '"duration": "13.38", "snr_value": 38.432806, "status": "Clean"}'
        self.assertEqual(expected_utterances, actual_utterances)

    @mock.patch('common.postgres_db_client.PostgresClient')
    def test_update_utterances(self, mock_postgres_client):
        mock_postgres_client.execute_update.return_value = None
        catalogueDao = CatalogueDao(mock_postgres_client)
        audio_id = '2020'
        utterances = '[{"name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav", ' \
                              '"duration": "13.38", "snr_value": 38.432806, "status": "Clean"}'

        rows_updated = catalogueDao.update_utterances(audio_id, utterances)
        self.assertEqual(rows_updated, True)