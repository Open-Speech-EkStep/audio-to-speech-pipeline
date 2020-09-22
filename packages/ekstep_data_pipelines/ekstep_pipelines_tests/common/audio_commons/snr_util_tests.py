import sys
import unittest
# from audio_processing import constants
from unittest.mock import Mock


from common.audio_commons.snr_util import SNR
sys.path.insert(0, '..')

class SNRTests(unittest.TestCase):

    def setUp(self):
        self.test_audio_file_path = "ekstep_pipelines_tests/resources/chunk.wav"
        self.audio_commons = {"snr_util": Mock(), "chunking_conversion": Mock()}
        self.snr = SNR()

    def test__should_return_command_when_get_command_called_with_file_path_and_dir(self):
        command ='"test_dir/binaries/WadaSNR/Exe/WADASNR" -i "input_file_path" -t "test_dir/binaries/WadaSNR/Exe/Alpha0.400000.txt" -ifmt mswav'
        actual_output = self.snr.get_command("test_dir","input_file_path")
        
        self.assertEqual(actual_output,command)

    # def test__should_return_SNR_of_given_file(self):
    #     # actual_snr = self.snr.compute_file_snr(self.test_audio_file_path)
    #     a = self.snr.compute_file_snr("/Users/heerabal/speech-recognition/audio-to-speech-pipeline/packages/ekstep_data_pipelines/ekstep_pipelines_tests/resources/test1.wav")
    #     # b = self.snr.compute_file_snr("ekstep_pipelines_tests/resources/test2.wav")
    #     # c = self.snr.compute_file_snr("ekstep_pipelines_tests/resources/test3.wav")

    #     # self.assertEqual(actual_snr,5)
    #     self.assertEqual(a,5)
    #     # self.assertEqual(b,5)
    #     # self.assertEqual(c,5)

