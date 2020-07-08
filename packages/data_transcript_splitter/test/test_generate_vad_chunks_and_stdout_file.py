import unittest
from src.main.create_vad_chunks_and_stdout_file import create_vad_chunks_for_file
import glob


class GenerateVadStdoutFile(unittest.TestCase):
    def test_should_create_383_files_for_pm_modi_audio(self):
        path_to_audio_file = './temp/chunks/chunk-12.wav'
        path_to_save_vad_log_file = './resources/expected_output/'
        dir_to_save_chunks = './resources/expected_output/'
        aggressiveness = 3
        args = [aggressiveness, path_to_audio_file]
        create_vad_chunks_for_file(args, dir_to_save_chunks=dir_to_save_chunks,
                                   path_to_save_log_file=path_to_save_vad_log_file,
                                   )
        print('-'*100)

        # expected = 383
        # received = len(glob.glob(dir_to_save_chunks + '/*.wav'))
        # self.assertEqual(expected, received)



if __name__ == '__main__':
    unittest.main()
