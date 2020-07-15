import os
import unittest

from src.scripts.vad_audio_clipper import create_audio_clips


class VadAudioClipper(unittest.TestCase):
    def test_vad_clip(self):
        aggressiveness = 2
        output_dir = './src/tests/test_resources/output'
        input_dir = './src/tests/test_resources/input'
        file_name = 'test.wav'
        input_file_path = input_dir + '/' + file_name
        vad_output_file_path = input_dir + '/' + file_name.replace('.wav', '_vad_output.txt')
        create_audio_clips(aggressiveness, input_file_path, output_dir, vad_output_file_path)
        clips = os.listdir(output_dir)  # dir is your directory path
        self.assertEqual(37, len(clips))
        # self.delete_files(output_dir)

    def delete_files(self, output_dir):
        files = os.listdir(output_dir)  # dir is your directory path
        for f in files:
            try:
                os.remove(output_dir + '/' + f)
            except OSError as e:
                print("Error: %s : %s" % (f, e.strerror))


if __name__ == '__main__':
    unittest.main()