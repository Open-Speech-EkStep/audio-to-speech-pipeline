import sys
import unittest

from common.file_utils import get_file_name

sys.path.insert(0, '..')


class FileUtilsTests(unittest.TestCase):

    def test_get_file_name(self):
        file_name = get_file_name('ekstep_pipelines_tests/resources/chunk.wav')
        self.assertEquals('chunk.wav', file_name)
