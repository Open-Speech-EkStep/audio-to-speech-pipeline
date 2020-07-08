import unittest
from src.main.word_to_time_mapping_srt import map_word_to_time


class WordToTimeMapping(unittest.TestCase):
    def test_should_map_word_to_time(self):
        wordStartAndEnd = ['',
                           'seconds: 1    nanos: 200000000',
                           'seconds: 1    nanos: 200000000',
                           'seconds: 1    nanos: 300000000',
                           ]

        words = ['word1', 'word2']

        expected = [('word1', 0, 1.2), ('word2', 1.2, 1.3)]
        received = map_word_to_time(words, wordStartAndEnd)

        self.assertEqual(expected, received)


if __name__ == '__main__':
    unittest.main()
