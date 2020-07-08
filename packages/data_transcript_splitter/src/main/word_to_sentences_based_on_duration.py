from src.main.word_srt import Word
from src.main.chunk_vad import VadChunk
import math


def word_to_sentence(words_object_list_stt, vad_chunks_object_list):
    sentences = []
    last_word_index = 0
    index = 0

    for chunk in vad_chunks_object_list:
        chunk_name = chunk.name
        flag = False

        chunk
        sentence_words = []
        flag = False

        for index, word_obj in enumerate(words_time_map_stt):
            if index < last_word_index:
                continue
            word = word_obj.word
            start_time = word_obj.start_time
            end_time = word_obj.end_time
            word_duration = end_time - start_time
            if word_duration > 1:
                word_duration = word_duration - 0.4

            if word_duration == 0:
                word_duration = 0.1

            if sentence_duration + word_duration <= chunk_duration:
                sentence_duration += word_duration
                sentence_words.append(word)
            else:
                sentences.append(' '.join(sentence_words))
                sentence_words = [word]
                sentence_duration = word_duration
                last_word_index = index
                flag = True
                break
        if flag:
            continue

    return sentences