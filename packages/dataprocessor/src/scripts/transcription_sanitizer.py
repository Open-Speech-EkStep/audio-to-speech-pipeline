import re


class TranscriptionSanitizer(object):

    def sanitize(self, transcription):
        transcription = transcription.strip()  # removes spaces in starting of transcription
        if ':' in transcription:
            raise RuntimeError('transcription not acceptable for model training')

        transcription = self.replace_bad_char(transcription)
        transcription = transcription.strip()

        if len(transcription) == 0:
            raise RuntimeError('transcription not acceptable for model training')

        if self.shouldReject(transcription):
            raise RuntimeError('transcription not acceptable for model training')

        return transcription

    def shouldReject(self, transcription):
        valid_char = "[ ँ-ःअ-ऋए-ऑओ-नप-रलव-ह़ा-ृे-ॉो-्0-9क़-य़ ॅ]"
        rejected_string = re.sub(pattern=valid_char, repl='', string=transcription)
        if len(rejected_string.strip()) > 0:
            return True

        return False

    def replace_bad_char(self, transcription):

        if '-' in transcription:
            transcription = transcription.replace('-', ' ')

        punctuation = '₹!"#$%&\'()*+,./;<=>?@[\\]^_`{|}~।'
        table = str.maketrans(dict.fromkeys(punctuation))
        return transcription.translate(table)
