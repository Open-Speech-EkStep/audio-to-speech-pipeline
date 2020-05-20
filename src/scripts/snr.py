import glob
import os, sys
import glob
import subprocess
import tempfile
import soundfile as sf
import numpy as np
from tqdm import tqdm
from multiprocessing import Pool

class SNR(object):

    def compute_file_snr(self, file_path, CURRENT_PATH):
        """ Convert given file to required format with FFMPEG and process with WADA."""
        _, sr = sf.read(file_path)
        # new_file = file_path.replace(".wav", "_tmp.wav")
        # if sr != 16000:
        #     command = f'ffmpeg -i "{file_path}" -ac 1 -acodec pcm_s16le -y -ar 16000 "{new_file}"'
        # else:
        #     command = f'cp "{file_path}" "{new_file}"'
        # os.system(command)
        #print('here')
        #command = [f'"{CURRENT_PATH}/WadaSNR/Exe/WADASNR"', f'-i "{new_file}"', f'-t "{CURRENT_PATH}/WadaSNR/Exe/Alpha0.400000.txt"', '-ifmt mswav']
        command = f'"{CURRENT_PATH}/WadaSNR/Exe/WADASNR" -i "{file_path}" -t "{CURRENT_PATH}/WadaSNR/Exe/Alpha0.400000.txt" -ifmt mswav'
        output = subprocess.check_output(command, shell=True)
        try:
            output = float(output.split()[-3].decode("utf-8"))
        except:
            raise RuntimeError(" ".join(command))
        #os.system(f'rm "{new_file}"')
        print(output)
        return output


    def fit(self, input_file_dir):
        wav_files = input_file_dir
        file_snrs = {}

        CURRENT_PATH = '../..'
        for file in tqdm(wav_files):
            tup = self.compute_file_snr(file, CURRENT_PATH)
            file_snrs[file] = tup

        for key, value in file_snrs.items():
            print(f"File {key} has an snr value of {value}")
        return file_snrs

    def fit_and_move(self, input_file_dir, threshold, output_file_dir ):
        local_dict = self.fit(input_file_dir)

        print(local_dict)

        # if output_file_dir is None:
        #     output_file_dir = input_file_dir

        clean_dir = output_file_dir + '/clean'
        rejected_dir = output_file_dir + '/rejected'

        if not os.path.exists(clean_dir):
            os.mkdir(clean_dir)

        if not os.path.exists(rejected_dir):
            os.mkdir(rejected_dir)
        

        for key, value in local_dict.items():
            audio_file_name = key.split('/')[-1]
            print(audio_file_name)

            if value >= threshold:
                ## copy to clean directory of output
                clean_dir_local = clean_dir + '/' + audio_file_name 
                print(clean_dir_local)
                command = f'mv "{key}" "{clean_dir_local}"'
            else:
                ## copy to rejected directory of output
                rejected_dir_local = rejected_dir + '/' + audio_file_name
                print(rejected_dir_local)
                command = f'mv "{key}" "{rejected_dir_local}"'

            os.system(command)
            output = subprocess.check_output(command, shell=True)


if __name__ == "__main__":
    snr_obj = SNR()
    input_file_dir = '/home/harveen.chadha/gcmount/data/audiotospeech/raw/landing/hindi/audio/testing/cutaudio'
    threshold = 20
    snr_obj.fit_and_move(input_file_dir , threshold)