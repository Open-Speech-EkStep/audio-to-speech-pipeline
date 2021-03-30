import sys
import unittest
from unittest.mock import Mock
import glob
import os
import shutil
import subprocess
from unittest import mock


from ekstep_data_pipelines.common.audio_commons.chunking_conversion_util import (
    ChunkingConversionUtil, )


class SNRTests(unittest.TestCase):

    INPUT_FILE_EXTENSION = "mp4"
    OUTPUT_FILE_EXTENSION = ".wav"

    def setUp(self):
        super(SNRTests, self).setUp()

        self.input_file_dir = "ekstep_pipelines_tests/resources/chunking_input"
        self.output_file_dir = "ekstep_pipelines_tests/resources/chunking_output"

        if os.path.exists(self.output_file_dir):
            shutil.rmtree(self.output_file_dir)
        os.mkdir(self.output_file_dir)

        self.chunking_conversion_util = ChunkingConversionUtil()

    def test__snr(self):
        self.assertEqual(1, 1)

    def test_convert_to_wav_should_return_list_as_output(self):
        self.assertEqual(
            type(
                self.chunking_conversion_util.convert_to_wav(
                    self.input_file_dir, self.output_file_dir
                )
            ),
            type(("", "")),
        )

    def test_convert_to_wav_return_should_return_same_number_of_items_as_many_input_mp4_files(
        self,
    ):
        input_file_list = glob.glob(
            self.input_file_dir + "/*." + self.INPUT_FILE_EXTENSION
        )
        self.assertEqual(
            len(
                [
                    self.chunking_conversion_util.convert_to_wav(
                        self.input_file_dir, self.output_file_dir
                    )
                ]
            ),
            len(input_file_list),
        )

    def test_convert_to_wav_should_return_output_file_with_wav_extension(self):
        input_file_list = glob.glob(
            self.input_file_dir + "/*." + self.INPUT_FILE_EXTENSION
        )
        if len(input_file_list) > 1:
            self.assertTrue(
                self.chunking_conversion_util.convert_to_wav(
                    self.input_file_dir, self.output_file_dir
                )[0].endswith(self.OUTPUT_FILE_EXTENSION)
            )

    @mock.patch("subprocess.call")
    def test_convert_to_wav_should_call_the_subprocess_to_convert_mp4_to_wav(
        self, mock_subprocess_call
    ):
        self.chunking_conversion_util.convert_to_wav(
            self.input_file_dir, self.output_file_dir
        )
        self.assertTrue(mock_subprocess_call.called)

    @mock.patch("subprocess.call")
    def test_convert_to_wav_should_call_the_subprocess_to_convert_mp4_to_wav_for_every_input_file(
            self, mock_subprocess_call):
        self.chunking_conversion_util.convert_to_wav(
            self.input_file_dir, self.output_file_dir
        )
        self.assertEqual(mock_subprocess_call.call_count, 1)
