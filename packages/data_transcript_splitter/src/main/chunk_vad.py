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
        print('Sentence:', self.sentence, '\nStart time:', self.start_time, '\nEnd time:', self.end_time, '\nName:', self.name)