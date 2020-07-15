import unittest

from src.scripts.merge_chunks import merge_chunks


class MergeChunks(unittest.TestCase):
    def test_merge_chunks(self):
        chunks_dir = './src/tests/test_resources/input/chunks'
        out_dir = './src/tests/test_resources/output/merged'
        separator_file_path = './src/tests/test_resources/input/chunks' + '/separator/hoppi.wav'
        merged_file_path = merge_chunks(chunks_dir, out_dir, separator_file_path, 'chunk')
        self.assertEquals('merged.wav', merged_file_path)

if __name__ == '__main__':
    unittest.main()
