import sys
import unittest

from audio_analysis.speaker_analysis.create_embeddings import encoder
from audio_analysis.speaker_analysis.speaker_clustering import create_speaker_clusters
from os import path

sys.path.insert(0, '..')


class AudioSpeakerClusteringTests(unittest.TestCase):

    def test_should_create_embeddings_for_wav_files_set(self):
        source_path = 'ekstep_pipelines_tests/resources/test_source/'
        embed_file_name = '/tmp/embed_map.npz'
        encoder(source_dir=source_path,
                source_dir_pattern='*/clean/*.wav',
                embed_file_name=embed_file_name)
        self.assertTrue(path.exists(embed_file_name))

    def test_should_map_speakers_to_utterance_for_given_npz(self):
        embed_file_name ='ekstep_pipelines_tests/resources/embed_map.npz'
        file_map_dict, noise_file_map_dict = create_speaker_clusters(
            embed_filename_map_path=embed_file_name,
            source_name='mkb')
        print(len(file_map_dict))
        print(file_map_dict)
        self.assertEqual(5, len(file_map_dict))
