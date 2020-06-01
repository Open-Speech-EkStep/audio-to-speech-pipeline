import os
import shutil
import unittest
import sys
import pandas as pd
from unittest import mock
from unittest.mock import MagicMock, patch, Mock
# sys.path.append("../../src")
from unittest import TestCase

from pydub import AudioSegment
from src.scripts.audio_clipper import AudioClipper, Data


class TestAudioClipper(unittest.TestCase):
    OUTPUT_FILE_EXTENSION = '.wav'
    METADATA_COLUMN = 'utterances_file_list'
    SRT_TEXT_VALUE = 'क्या मुझे मालूम है जिस तरह किसी कुत्ते को खींचा जाता है इसी तरह मेरे को भी जीने से खींचते वक्त पर डाल दिया\n'

    def setUp(self):
        super(TestAudioClipper, self).setUp()
        self.ca = AudioClipper()
        self.data_obj = Data()
        self.srt_file_path = 'src/tests/test_resources/input/test.srt'
        self.audio_file_path = 'src/tests/test_resources/input/test.wav'
        self.output_file_dir = 'src/tests/test_resources/output'

    def test_preprocess_srt_should_return_expected_value(self):
        obj = self.ca.preprocess_srt(self.srt_file_path)
        self.assertEqual(obj[1].text, self.SRT_TEXT_VALUE)

    def test_preprocess_srt_should_return_object_with_list_type(self):
        obj = self.ca.preprocess_srt(self.srt_file_path)
        self.assertEqual(type(obj), type([]))

    def test_preprocess_srt_should_return_list_containing_Data_object(self):
        obj = self.ca.preprocess_srt(self.srt_file_path)
        self.assertEqual(type(obj[1]), type(self.data_obj))

    def test_make_directories_should_return_value_as_1_when_directory_already_present(self):
        path = self.output_file_dir
        self.assertEqual(self.ca.make_directories(path), 1)

    def test_clip_audio_with_ffmeg_should_return_object_with_list_type(self):
        shutil.rmtree(self.output_file_dir)
        os.mkdir(self.output_file_dir)
        list_objs = self.ca.preprocess_srt(self.srt_file_path)
        files_written = self.ca.clip_audio_with_ffmeg(list_objs, self.audio_file_path, self.output_file_dir)[0]
        self.assertEqual(type(files_written), type([]))

    def test_clip_audio_with_ffmeg_should_return_number_of_files_written_equal_to_number_of_items_in_srt_file(self):
        shutil.rmtree(self.output_file_dir)
        os.mkdir(self.output_file_dir)
        list_objs = self.ca.preprocess_srt(self.srt_file_path)
        files_written = self.ca.clip_audio_with_ffmeg(list_objs, self.audio_file_path, self.output_file_dir)[0]
        self.assertEqual(len(files_written), len(list_objs))

    def test_clip_audio_with_ffmeg_files_should_return_true_when_metadata_file_is_generated(self):
        shutil.rmtree(self.output_file_dir)
        os.mkdir(self.output_file_dir)
        list_objs = self.ca.preprocess_srt(self.srt_file_path)
        metadata_file_name = self.ca.clip_audio_with_ffmeg(list_objs, self.audio_file_path, self.output_file_dir)[1]
        self.assertTrue(os.path.exists(metadata_file_name))

    def test_clip_audio_with_ffmeg_files_should_return_metadata_file_column_with_list_of_files_written(self):
        shutil.rmtree(self.output_file_dir)
        os.mkdir(self.output_file_dir)
        list_utterances_files = []
        list_objs = self.ca.preprocess_srt(self.srt_file_path)
        files_written, metadata_file_name = self.ca.clip_audio_with_ffmeg(list_objs, self.audio_file_path,
                                                                          self.output_file_dir)
        for file in files_written:
            list_utterances_files.append(file.split('/')[-1].split('.')[0] + self.OUTPUT_FILE_EXTENSION)

        metadata_file_content = pd.read_csv(metadata_file_name)[self.METADATA_COLUMN].to_list()[0]
        self.assertEqual(str(list_utterances_files), metadata_file_content)

    @mock.patch('pydub.AudioSegment.from_wav')
    def test_clip_audio_with_ffmeg_should_call_the_from_wav_method(self, mock_audio_segment):
        list_objs = self.ca.preprocess_srt(self.srt_file_path)
        self.ca.clip_audio_with_ffmeg(list_objs, self.audio_file_path,
                                      self.output_file_dir)
        self.assertTrue(mock_audio_segment.called)


if __name__ == '__main__':
    unittest.main()
