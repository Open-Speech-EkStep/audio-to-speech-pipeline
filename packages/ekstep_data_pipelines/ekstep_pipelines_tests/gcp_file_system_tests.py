import unittest
import sys
from unittest.mock import Mock

from common.file_system.gcp_file_systen import GCPFileSystem

sys.path.insert(0, '..')


class DataMoverTests(unittest.TestCase):

    def setUp(self):
        self.gcp_operations = Mock()
        self.gcp_file_system = GCPFileSystem(self.gcp_operations)


    def test__should_list_files(self):
        dir = 'gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/catalogued/hindi/audio/swayamprabha_chapter/1/clean'
        self.gcp_operations.list_blobs_in_a_path.return_value = [Path('path1'), Path('path2')]
        files = self.gcp_file_system.ls(dir)
        call_args = self.gcp_operations.list_blobs_in_a_path.call_args
        self.assertEqual(call_args[0][0], dir)
        self.assertEqual(['path1', 'path2'], files)

class Path:
    def __init__(self, path):
        self.name = path