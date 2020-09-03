import sys
import unittest
from unittest import mock

from dao.catalogue_dao import CatalogueDao

sys.path.insert(0, '..')

class CatalogueTests(unittest.TestCase):

    @mock.patch('common.postgres_db_client.PostgresClient')
    def test_get_utterances(self, mock_data_processor):
        mock_data_processor.execute_query.return_value = '[{"name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav", ' \
                              '"duration": "13.38", "snr_value": 38.432806, "status": "Clean"}'

        catalogueDao = CatalogueDao(mock_data_processor)
        audio_id = '2020'
        actual_utterances = catalogueDao.get_utterances(audio_id)
        expected_utterances = '[{"name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav", ' \
                              '"duration": "13.38", "snr_value": 38.432806, "status": "Clean"}'
        self.assertEqual(expected_utterances, actual_utterances)