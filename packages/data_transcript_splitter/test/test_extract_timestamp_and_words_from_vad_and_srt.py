import unittest
import re
from src.main.extract_timestamps_and_words_from_vad_and_srt import Extract
import pickle


class ExtractTimeStampAndWords(unittest.TestCase):
    def test_should_return_list_of_383_chunk_objects_from_vad_log_file(self):
        # expected = [['0.12', '0.9900000000000007'],
        #             ['1.0200000000000007', '4.799999999999995'],
        #             ['4.829999999999996', '11.519999999999946'],
        #             ['11.669999999999943', '14.009999999999893'],
        #             ['14.039999999999893', '16.739999999999878']
        #             ]

        path_vad_stdout = './temp/pm_modi_processed_vad_output.txt'

        extract = Extract()
        received = extract.chunks_objects_list_from_vad_output(path_vad_stdout)
        self.assertEqual(383, len(received))
        self.assertTrue('<src.main.chunk_vad.VadChunk object at' in str(received[0]))

    def test_should_return_list_of_3605_word_objects_from_google_srt_response(self):
        path_srt_file = './temp/mankibaat_may.txt'

        extract = Extract()
        received = extract.extract_timestamp_srt(path_srt_file)
        self.assertEqual(3605, len(received))
        self.assertTrue('<src.main.word_srt.Word object at' in str(received[0]))


if __name__ == '__main__':
    unittest.main()
