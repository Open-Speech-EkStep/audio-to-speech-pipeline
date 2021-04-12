import unittest
from unittest import mock
from unittest.mock import Mock
import os


from ekstep_data_pipelines.audio_embedding.audio_embedding import AudioEmbedding



class AudioEmbeddingsTests(unittest.TestCase):

    def setUp(self):
        self.postgres_client = Mock()
        self.gcp_instance = Mock()

        self.audio_embedding = AudioEmbedding(
            self.postgres_client
        )

        self.audio_embedding.fs_interface = Mock()

    def test_should_create_embeddings_for_given_audios(self):

        self.audio_embedding.create_embeddings('*.wav','test.npz','ekstep_pipelines_tests/resources/test_source/123/clean/','ekstep_pipelines_tests/resources/test_source/123/clean/')

        self.assertTrue(os.path.exists("ekstep_pipelines_tests/resources/test_source/123/clean/test.npz"))

    def test_upload_to_gcp(self):
        self.audio_embedding.upload_to_gcp('local_file_path','input_file_path')

        self.assertEqual(
            self.audio_embedding.fs_interface.upload_to_location.call_count, 1
        )
