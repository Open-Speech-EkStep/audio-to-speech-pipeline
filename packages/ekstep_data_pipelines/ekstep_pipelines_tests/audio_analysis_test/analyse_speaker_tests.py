import sys
import unittest
from unittest import mock

from audio_analysis.analyse_speaker import analyse_speakers
from audio_analysis.speaker_analysis.create_embeddings import encoder

sys.path.insert(0, '..')

class AnalyseSpeakersTests(unittest.TestCase):

    @mock.patch('audio_analysis.speaker_analysis.create_embeddings.encoder')
    @mock.patch('audio_analysis.speaker_analysis.create_embeddings.encoder')
    @mock.patch('audio_analysis.speaker_analysis.speaker_clustering.create_speaker_clusters')
    @mock.patch('audio_analysis.speaker_analysis.file_cluster_mapping.speaker_to_file_name_map')
    def test_should_analyse_speakers_for_source(self, speaker_to_file_name_map_mock, create_speaker_clusters_mock, encoder_mock, catalogue_dao):
        encoder_mock.return_value = None
        create_speaker_clusters_mock.return_value = None, None
        speaker_to_file_name_map_mock.return_value = {}
        catalogue_dao.insert_speaker.return_value = True
        catalogue_dao.update_utterance_speaker.return_value = True
        catalogue_dao.select_speaker.return_value = -1
        source_path = 'ekstep_pipelines_tests/resources/test_source/'
        embed_file_name = '/tmp/embed_map.npz'
        analyse_speakers(embed_file_name, '*/clean/*.wav', source_path, 'test_source', catalogue_dao, min_cluster_size=2, partial_set_size=11122, min_samples=2)
        insert_args = catalogue_dao.insert_speaker.call_args
        update_args = catalogue_dao.update_utterance_speaker.call_args
        self.assertEqual('test_source_sp_1', insert_args[0][1])
        self.assertEqual('test_source', insert_args[0][0])
        self.assertEqual(3, len(update_args[0][0]))
        self.assertEqual('test_source_sp_1', update_args[0][1])
