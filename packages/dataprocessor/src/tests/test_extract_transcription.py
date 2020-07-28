import os
import pickle
import shutil
import unittest

from src.scripts.extract_transcription import extract_transcription

from src.scripts.save_transcription import save_transcriptions


class ExtractTranscription(unittest.TestCase):
    def test_extract_sentences(self):
        path_srt_file = './src/tests/test_resources/input/merged.txt'
        with open(path_srt_file, 'rb') as file:
            content = pickle.load(file)
        transcriptions = extract_transcription(content)
        print(transcriptions)
        print(len(transcriptions))
        output_path = './src/tests/test_resources/output/SampleHindi/'
        os.makedirs(output_path)
        save_transcriptions(output_path, transcriptions, 'chunk')
        if os.path.exists(output_path):
            shutil.rmtree(output_path)
        self.assertEqual(3, len(transcriptions))
        self.assertEqual('', transcriptions[0])
        self.assertEqual('मेरे प्यारे देशवासियों नमस्कार', transcriptions[1])
        self.assertEqual('कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है', transcriptions[2])

if __name__ == '__main__':
    unittest.main()

