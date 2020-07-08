from src.main.word_srt import Word
from src.main.chunk_vad import VadChunk
import math


def word_to_sentence(words_object_list_stt, vad_chunks_object_list):
    # vad_chunk_durations = list(map(to_vad_durations, vad_chunks_start_and_end_time))
    sentences = []
    last_word_index = 0
    index = 0
    # for chunk_duration in vad_chunk_durations:
    chunk_sentence_map = []
    for chunk in vad_chunks_object_list:
        chunk_name = chunk.name
        flag = False
        sentence_words = []
        start_time_chunk = chunk.start_time
        end_time_chunk = chunk.end_time
        local_word_obj = []
        for index, word_obj in enumerate(words_object_list_stt):
            # if index < last_word_index:
            #     continue
            word = word_obj.word
            start_time_word = word_obj.start_time
            end_time_word = word_obj.end_time
            # print('st word', start_time_word)
            # print('st chunk', start_time_chunk)

            if start_time_word >= start_time_chunk and end_time_word <= end_time_chunk:
                sentence_words.append(word)
                local_word_obj.append(word_obj)
            else:
                continue
                # sentences.append(' '.join(sentence_words))
                # sentence_words = [word]
                # last_word_index = index
                # flag = True
                # break
        word_objects = []
        for w in words_object_list_stt:
            if w not in local_word_obj:
                word_objects.append(w)

        sentences.append(' '.join(sentence_words))
        words_object_list_stt = word_objects
    with open('./temp/words_dropped_new.txt', 'w+') as f:
        for w in words_object_list_stt:
            f.write(str(w.word)+ ', ' +str(w.start_time)+', '+str(w.end_time))
            f.write('\n')
    print(len(words_object_list_stt))
        # if flag:
        #     continue
    return sentences
