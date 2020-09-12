import unittest
import sys
from unittest.mock import Mock

from data_marker.data_mover import MediaFilesMover

sys.path.insert(0, '..')


class DataMoverTests(unittest.TestCase):

    def setUp(self):
        self.file_system = Mock()
        self.media_files_mover = MediaFilesMover(self.file_system)


    def test__should_move_media_files(self):
        source_base_path = 'gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/catalogued/hindi/audio/swayamprabha_chapter'
        landing_base_path = 'gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/landing/hindi/audio/swayamprabha_chapter'
        audio_ids = ['1']
        self.media_files_mover.move_media_files(source_base_path, landing_base_path, audio_ids)
        call_args_list = self.file_system.mv.call_args_list

        self.assertEqual(call_args_list[0][0][0], f'{source_base_path}/1/clean')
        self.assertEqual(call_args_list[0][0][1], f'{landing_base_path}/1')

        self.assertEqual(call_args_list[1][0][0], f'{source_base_path}/1/rejected')
        self.assertEqual(call_args_list[1][0][1], f'{landing_base_path}/1')

