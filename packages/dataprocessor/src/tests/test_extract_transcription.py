import os
import pickle
import shutil
import unittest

from src.scripts.extract_transcription import extract_transcription, save_transcriptions


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
        self.assertEqual(3, len(transcriptions))
        self.assertEqual('', transcriptions[0])
        self.assertEqual('मेरे प्यारे देशवासियों नमस्कार', transcriptions[1])
        self.assertEqual('कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है', transcriptions[2])


    def test_save_transcriptions(self):
        transcriptions = ['', 'मेरे प्यारे देशवासियों', 'नमस्कार', '', 'कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है', 'जब मैंने पिछली बार आपसे']
        output_file_dir = './test_resources/output/transcriptions'
        os.mkdir(output_file_dir)
        save_transcriptions(output_file_dir, transcriptions, 'chunk')
        files = os.listdir(output_file_dir)
        if os.path.exists(output_file_dir):
            shutil.rmtree(output_file_dir)
        self.assertEqual(len(files), 6)
        self.assertEqual(files[0], 'chunk-0.txt')

if __name__ == '__main__':
    unittest.main()

