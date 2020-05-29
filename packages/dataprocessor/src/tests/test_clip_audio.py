import os
import shutil
import unittest
import sys
import pandas as pd
# sys.path.append("../../src")
from unittest import TestCase

from src.scripts.clip_audio import ClipAudio, Data


class TestClipAudio(unittest.TestCase):

    def setUp(self):
        super(TestClipAudio, self).setUp()
        self.data_obj = Data()
        self.srt_file_path = 'src/tests/test_resources/input/test.srt'
        self.audio_file_path = 'src/tests/test_resources/input/test.wav'
        self.output_file_dir = 'src/tests/test_resources/output'


    def test_preprocess_srt_return_value(self):
        ca = ClipAudio()
        obj = ca.preprocess_srt(self.srt_file_path)
        self.assertEqual(obj[1].text,
                         'क्या मुझे मालूम है जिस तरह किसी कुत्ते को खींचा जाता है इसी तरह मेरे को भी जीने से खींचते वक्त पर डाल दिया\n')

    def test_preprocess_srt_return_type(self):
        ca = ClipAudio()
        obj = ca.preprocess_srt(self.srt_file_path)
        self.assertEqual(type(obj), type([]))

    def test_preprocess_srt_return_value_type(self):
        ca = ClipAudio()
        obj = ca.preprocess_srt(self.srt_file_path)
        self.assertEqual(type(obj[1]), type(self.data_obj))

    def test_make_directories_when_directory_already_present(self):
        ca = ClipAudio()
        path = self.output_file_dir
        self.assertEqual(ca.make_directories(path), 1)

    def test_clip_audio_with_ffmeg_files_written_return_type(self):
        shutil.rmtree(self.output_file_dir)
        os.mkdir(self.output_file_dir)
        ca = ClipAudio()
        list_objs = ca.preprocess_srt(self.srt_file_path)
        files_written = ca.clip_audio_with_ffmeg(list_objs, self.audio_file_path, self.output_file_dir)[0]
        self.assertEqual(type(files_written), type([]))

    def test_clip_audio_with_ffmeg_files_written_count(self):
        shutil.rmtree(self.output_file_dir)
        os.mkdir(self.output_file_dir)
        ca = ClipAudio()
        list_objs = ca.preprocess_srt(self.srt_file_path)
        files_written = ca.clip_audio_with_ffmeg(list_objs, self.audio_file_path, self.output_file_dir)[0]
        self.assertEqual(len(files_written), len(list_objs))

    def test_clip_audio_with_ffmeg_files_written_metadata_file(self):
        shutil.rmtree(self.output_file_dir)
        os.mkdir(self.output_file_dir)
        ca = ClipAudio()
        list_objs = ca.preprocess_srt(self.srt_file_path)
        metadata_file_name = ca.clip_audio_with_ffmeg(list_objs, self.audio_file_path, self.output_file_dir)[1]
        self.assertTrue(os.path.exists(metadata_file_name))

    def test_clip_audio_with_ffmeg_files_written_metadata_contents(self):
        shutil.rmtree(self.output_file_dir)
        os.mkdir(self.output_file_dir)
        ca = ClipAudio()
        list_objs = ca.preprocess_srt(self.srt_file_path)
        files_written,metadata_file_name = ca.clip_audio_with_ffmeg(list_objs, self.audio_file_path, self.output_file_dir)
        metadata_file_content=pd.read_csv(metadata_file_name)['utterances_file_list'].to_list()[0]
        self.assertEqual(str(files_written),metadata_file_content)


if __name__ == '__main__':
    unittest.main()


