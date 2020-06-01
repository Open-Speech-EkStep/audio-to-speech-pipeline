import glob
import os
import shutil
import unittest

from src.scripts.srt_generator import SRTGenerator


class TestSRTGenerator(unittest.TestCase):
    INPUT_FILE_EXTENSION = 'mp4'
    OUTPUT_FILE_EXTENSION = '.wav'

    def setUp(self):
        super(TestSRTGenerator, self).setUp()
        self.srt = SRTGenerator('hi')
        self.srt_file_path = 'src/tests/test_resources/input/test.srt'
        self.audio_file_path = 'src/tests/test_resources/input/test.wav'
        self.input_file_dir = 'src/tests/test_resources/input'
        self.output_file_dir = 'src/tests/test_resources/output'

    def test_convert_to_wav_should_return_list_as_output(self):
        shutil.rmtree(self.output_file_dir)
        os.mkdir(self.output_file_dir)
        self.assertEqual(type(self.srt.convert_to_wav(self.input_file_dir, self.output_file_dir)), type([]))

    def test_convert_to_wav_return_should_return_same_number_of_items_as_many_input_mp4_files(self):
        shutil.rmtree(self.output_file_dir)
        os.mkdir(self.output_file_dir)
        input_file_list = glob.glob(self.input_file_dir + '/*.' + self.INPUT_FILE_EXTENSION)
        self.assertEqual(len(self.srt.convert_to_wav(self.input_file_dir, self.output_file_dir)), len(input_file_list))

    def test_convert_to_wav_should_return_output_file_with_wav_extension(self):
        shutil.rmtree(self.output_file_dir)
        os.mkdir(self.output_file_dir)
        input_file_list = glob.glob(self.input_file_dir + '/*.' + self.INPUT_FILE_EXTENSION)
        if len(input_file_list) > 1:
            self.assertTrue(self.srt.convert_to_wav(self.input_file_dir, self.output_file_dir)[0].endswith(
                self.OUTPUT_FILE_EXTENSION))


if __name__ == '__main__':
    unittest.main()
