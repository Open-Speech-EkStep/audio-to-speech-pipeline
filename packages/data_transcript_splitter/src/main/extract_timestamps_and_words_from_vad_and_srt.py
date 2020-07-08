import re
import pandas as pd
import pickle
from src.main.word_srt import Word
from src.main.chunk_vad import VadChunk


# processing the file: vad_stdout.txt
class Extract(object):

    def __init__(self):
        self.__set_search_patterns()

    def __set_search_patterns(self):
        self.pattern_for_text_within_parenthesis = re.compile('\((.*?)\)')

    def chunks_objects_list_from_vad_output(self, path_vad_stdout):
        with open(path_vad_stdout) as file:
            content = file.readlines()

        vad_chunks = []
        for i, line in enumerate(content):
            chunk_obj = VadChunk(0, 0)
            if i % 2 == 0:
                start_and_end_time = re.findall(self.pattern_for_text_within_parenthesis, line)
                if start_and_end_time:
                    start = float(start_and_end_time[0])
                    end = float(start_and_end_time[1])
            else:
                name = line.replace('Writing', '')
                name = name.strip()
                chunk_obj.set_start_time(start)
                chunk_obj.set_end_time(end)
                chunk_obj.set_name(name)
                # chunk_obj.print()
                vad_chunks.append(chunk_obj)

        return vad_chunks

    def extract_timestamp_srt(self, path_srt):
        with open(path_srt, 'rb') as file:
            content = pickle.load(file)
        c = 0
        words_data = []
        for i in content.results:
            for w in i.alternatives[0].words:
                word = w.word
                c += 1
                # if not w.start_time.seconds:

                start = w.start_time.seconds + w.start_time.nanos / pow(10, 9)
                end = w.end_time.seconds + w.end_time.nanos / pow(10, 9)
                word_obj = Word(word, start, end)
                words_data.append(word_obj)
                # word_obj.print()

        return words_data
