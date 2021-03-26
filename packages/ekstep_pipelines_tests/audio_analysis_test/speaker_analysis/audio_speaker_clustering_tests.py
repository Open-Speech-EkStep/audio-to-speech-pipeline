import sys
import unittest

from ekstep_data_pipelines.audio_analysis.speaker_analysis.create_embeddings import (
    encode_on_partial_sets,
)
from ekstep_data_pipelines.audio_analysis.speaker_analysis.speaker_clustering import (
    create_speaker_clusters,
)
from os import path

# sys.path.insert(0, '..')


class AudioSpeakerClusteringTests(unittest.TestCase):
    def test_should_create_embeddings_for_wav_files_set(self):
        source_path = "ekstep_pipelines_tests/resources/test_source/"
        embed_file_name = "/tmp/embed_map.npz"
        encode_on_partial_sets(
            source_dir=source_path,
            source_dir_pattern="*/clean/*.wav",
            embed_file_path=embed_file_name,
            partial_set_size_for_embedding=11122
        )
        self.assertTrue(path.exists(embed_file_name))

    def test_should_map_speakers_to_utterance_for_given_npz(self):
        embed_file_name = "ekstep_pipelines_tests/resources/embed_map.npz"
        file_map_dict, noise_file_map_dict = create_speaker_clusters(
            embed_filename_map_path=embed_file_name,
            source_name="mkb",
            min_cluster_size=2,
            partial_set_size=200,
            min_samples=2,
        )
        print(len(file_map_dict))
        print(file_map_dict)
        self.assertEqual(2, len(file_map_dict))
