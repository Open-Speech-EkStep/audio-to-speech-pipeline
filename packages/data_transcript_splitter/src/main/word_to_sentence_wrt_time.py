from src.main.word_srt import Word
from src.main.chunk_vad import VadChunk
import math


def to_float(args):
    return float(args)


def to_vad_durations(vad_chunk):
    return vad_chunk.end_time - vad_chunk.start_time


def word_to_sentence(words_object_list_stt, vad_chunks_object_list):
    # vad_chunk_durations = list(map(to_vad_durations, vad_chunks_start_and_end_time))
    sentences = []
    last_word_index = 0
    index = 0
    # for chunk_duration in vad_chunk_durations:
    for chunk in vad_chunks_object_list:
        flag = False
        sentence_words = []
        end_time_chunk = round(chunk.end_time, 1)

        for index, word_obj in enumerate(words_object_list_stt):
            if index < last_word_index:
                continue

            word = word_obj.word
            end_time_word = word_obj.end_time

            if end_time_word <= end_time_chunk:
                sentence_words.append(word)
                if index == len(words_object_list_stt) - 1:
                    sentences.append(' '.join(sentence_words))

            else:
                sentences.append(' '.join(sentence_words))
                last_word_index = index
                flag = True
                break

        if flag:
            continue

    return sentences
