class Word(object):
    def __init__(self, word: str, start_time: float, end_time: float):
        self.word = word
        self.start_time = start_time
        self.end_time = end_time

    def print(self):
        print('Word:', self.word, '\nStart time:', self.start_time, '\nEnd time:', self.end_time)

    def set_start_time(self, time):
        self.start_time = time