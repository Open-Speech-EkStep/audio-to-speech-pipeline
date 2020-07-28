import os
import shutil
import unittest

from src.scripts.vad_audio_clipper import create_audio_clips


class VadAudioClipper(unittest.TestCase):
    def test_vad_clip(self):
        aggressiveness = 2
        output_dir = './src/tests/test_resources/output/chunks'
        input_dir = './src/tests/test_resources/input'
        file_name = 'test.wav'
        input_file_path = input_dir + '/' + file_name
        vad_output_file_path = input_dir + '/' + file_name.replace('.wav', '_vad_output.txt')
        base_chunk_name = 'test.wav'
        os.makedirs(output_dir)
        create_audio_clips(aggressiveness, input_file_path, output_dir, vad_output_file_path, base_chunk_name)
        actual_chunk_names = os.listdir(output_dir)  # dir is your directory path
        expected_chunk_names = [f'{i}_test.wav' for i in range(len(actual_chunk_names))]
        print('list:' + str(expected_chunk_names))
        self.delete_files(output_dir)
        self.assertEqual(set(actual_chunk_names), set(expected_chunk_names))

    def delete_files(self, output_dir):
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)


if __name__ == '__main__':
    unittest.main()