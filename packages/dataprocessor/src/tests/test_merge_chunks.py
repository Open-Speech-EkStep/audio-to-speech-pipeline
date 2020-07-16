import unittest

from src.scripts.merge_chunks import merge_chunks


class MergeChunks(unittest.TestCase):
    def test_merge_chunks(self):
        chunks_dir = './src/tests/test_resources/input/chunks'
        separator_file_path = './src/tests/test_resources/input/chunks' + '/separator/hoppi.wav'
        merged_file_path = merge_chunks(chunks_dir, separator_file_path, 'chunk', 'merged.wav')
        self.assertEquals('merged.wav', merged_file_path)

if __name__ == '__main__':
    unittest.main()
