import glob
import os
import subprocess
import soundfile as sf
from tqdm import tqdm

CURRENT_PATH = '../..'

class SNR(object):

    def compute_file_snr(self, file_path):
        """ Convert given file to required format with FFMPEG and process with WADA."""
        _, sr = sf.read(file_path)
        command = f'"{CURRENT_PATH}/WadaSNR/Exe/WADASNR" -i "{file_path}" -t "{CURRENT_PATH}/WadaSNR/Exe/Alpha0.400000.txt" -ifmt mswav'
        output = subprocess.check_output(command, shell=True)
        try:
            output = float(output.split()[-3].decode("utf-8"))
        except:
            raise RuntimeError(" ".join(command))
        return output

    def fit(self, input_file_dir):

        #wav_files = glob.glob(f"{input_file_dir}*wav")
        wav_files = input_file_dir
        print(wav_files)
        file_snrs = {}

        for file in tqdm(wav_files):
            tup = self.compute_file_snr(file)
            file_snrs[file] = tup

        for key, value in file_snrs.items():
            print(f"File {key} has an snr value of {value}")
        return file_snrs

    def fit_and_move(self, input_file_dir, threshold, output_file_dir):
        print(input_file_dir)
        local_dict = self.fit(input_file_dir)
        print(local_dict)
        clean_dir = output_file_dir + '/clean'
        rejected_dir = output_file_dir + '/rejected'

        if not os.path.exists(clean_dir):
            os.mkdir(clean_dir)

        if not os.path.exists(rejected_dir):
            os.mkdir(rejected_dir)

        for key, value in local_dict.items():
            audio_file_name = key.split('/')[-1]
            text_file_name_key = key[:-3] + 'txt' 
            text_file_name = key.split('/')[-1].split('.')[0] + '.txt'

            print(audio_file_name)
            command = ''
            command_text = ''

            if value >= threshold:
                ## copy to clean directory of output
                clean_dir_local = clean_dir + '/' + audio_file_name
                clean_dir_local_text = clean_dir + '/' + text_file_name

                command = f'mv "{key}" "{clean_dir_local}"'
                command_text = f'mv "{text_file_name_key}" "{clean_dir_local_text}"'
                print(command)
                print(command_text)
            else:
                ## copy to rejected directory of output
                rejected_dir_local = rejected_dir + '/' + audio_file_name
                rejected_dir_local_text = rejected_dir + '/' + text_file_name

                command = f'mv "{key}" "{rejected_dir_local}"'
                command_text = f'mv "{text_file_name_key}" "{rejected_dir_local_text}"'
                print(command)
                print(command_text)

            os.system(command + ';' + command_text)
            #os.system(command_text)
            #output = subprocess.check_output(command, shell=True)


if __name__ == "__main__":
    snr_obj = SNR()
    input_file_dir = '/home/anirudh/Projects/AudioSpeech/vad/'
    # input_file_dir = '/home/harveen.chadha/gcmount/data/audiotospeech/raw/landing/hindi/audio/testing/cutaudio'
    threshold = 16
    snr_obj.fit_and_move(input_file_dir, threshold, '/home/anirudh/Projects/AudioSpeech/vad/output')
