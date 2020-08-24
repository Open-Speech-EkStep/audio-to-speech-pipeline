

class TranscriptionSanitizer():

    def __init__(self):
        pass

    def sanitize(self, transcription):
        transcription = transcription.strip()
        transcription = self.replace_bad_char(transcription)
        if self.shouldReject(transcription):
            raise RuntimeError('transcription not acceptable for model training')

    def shouldReject(self, transcription):
        # implement this
        if transcription == '':
            return True
        else:
            return False

    def replace_bad_char(self, transcription):
        # handle this accoring to story AC
        punctuation = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~।'
        table = str.maketrans(dict.fromkeys(punctuation))
        return transcription.translate(table)

