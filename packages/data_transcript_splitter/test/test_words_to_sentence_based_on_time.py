import unittest
from src.main.word_to_sentence_wrt_time import word_to_sentence
from src.main.word_srt import Word
from src.main.chunk_vad import VadChunk
from src.main.extract_timestamps_and_words_from_vad_and_srt import Extract


class WordsToSentence(unittest.TestCase):
    def test_two_sample_words_should_be_mapped_to_sentence(self):
        word_1 = Word('word1', 0, 0.6)
        word_2 = Word('word2', 0.6, 1.3)
        word_3 = Word('word3', 1.3, 2.3)
        words_time_map_stt = [word_1, word_2, word_3]
        chunk_vad_1 = VadChunk(0, 1.3)
        chunk_vad_2 = VadChunk(1.5, 3.4)
        vad_chunks_start_and_end_time = [chunk_vad_1, chunk_vad_2]

        expected = ['word1 word2', 'word3']
        received = word_to_sentence(words_time_map_stt, vad_chunks_start_and_end_time)
        self.assertEqual(expected, received)

    def test_integration_on_entire_mankibaat_file(self):
        extract = Extract()

        path_vad_stdout = './temp/pm_modi_processed_vad_output.txt'
        chunk_objects_list_vad = extract.chunks_objects_list_from_vad_output(path_vad_stdout)
        # with open('./temp/check_chunk_objects.txt', 'w+') as f:
        #     for chunk in chunk_objects_list_vad:
        #         f.write(str(chunk.name)+', '+str(chunk.start_time)+', '+str(chunk.end_time))
        #         f.write('\n')

        # new_chunk_objects_list_vad = chunk_objects_list_vad[1:]

        path_srt_file = './temp/mankibaat_may.txt'
        word_objects_list_srt = extract.extract_timestamp_srt(path_srt_file)
        # word_objects_list_srt[0].set_start_time(0.9)
        # with open('./temp/check_word_objects_srt_start_changed.txt', 'w+') as f:
        #     for word in word_objects_list_srt:
        #         f.write(str(word.word)+', '+str(word.start_time)+', '+str(word.end_time))
        #         f.write('\n')

        received = word_to_sentence(word_objects_list_srt, chunk_objects_list_vad)
        with open('./temp/check_sentences_just_end.txt', 'w+') as f:
            i = 0
            for sent in received:
                f.write(str(i) + ' ' + str(sent))
                f.write('\n')
                i += 1


if __name__ == '__main__':
    unittest.main()
