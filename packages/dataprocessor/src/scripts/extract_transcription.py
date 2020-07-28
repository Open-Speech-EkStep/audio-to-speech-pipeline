import re

WORD_SEPERATOR = 'हॉपिपोला '

WORD_SEPERATOR_REGEX = 'हैप्पी पोला|हॉपिपोला|हॉपीपोला|फिर बोला'


def extract_transcription(content):
    original_transcription = list(map(lambda c: c.alternatives[0].transcript, content.results))
    print(' '.join(original_transcription))
    transcriptions = list(map(to_transcriptions, content.results))
    joined = ' '.join(transcriptions)
    corrected = re.sub(f'(({WORD_SEPERATOR_REGEX})(\s)(2,)){2,}', f'{WORD_SEPERATOR} ', joined)
    print(corrected)
    splitted = re.split(WORD_SEPERATOR_REGEX, corrected)
    trimmed = list(map(lambda s: s.strip(),
                       splitted))
    return trimmed


def to_transcriptions(chunk):
    transcript = re.sub(f'(({WORD_SEPERATOR_REGEX})(\s)){2,}', WORD_SEPERATOR,
                        chunk.alternatives[0].transcript)
    return transcript


def chunks_objects_list_from_vad_output(path_vad_stdout):
    pattern_for_text_within_parenthesis = re.compile('\((.*?)\)')
    with open(path_vad_stdout) as file:
        content = file.readlines()

    vad_chunks = []
    for i, line in enumerate(content):
        chunk_obj = VadChunk(0, 0)
        if i % 2 == 0:
            start_and_end_time = re.findall(pattern_for_text_within_parenthesis, line)
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


class VadChunk(object):
    def __init__(self, start_time: float, end_time: float, name=None, sentence=None):
        self.start_time = start_time
        self.end_time = end_time
        self.name = name
        self.sentence = sentence

    def set_start_time(self, stime):
        self.start_time = stime

    def set_end_time(self, etime):
        self.end_time = etime

    def set_name(self, name):
        self.name = name

    def print(self):
        print('Sentence:', self.sentence, '\nStart time:', self.start_time, '\nEnd time:', self.end_time, '\nName:',
              self.name)
