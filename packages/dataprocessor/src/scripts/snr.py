import glob
import os
import subprocess
import soundfile as sf
from tqdm import tqdm
import pandas as pd
import librosa
import math

CURRENT_PATH = os.getcwd()

class SNR(object):

    def compute_file_snr(self, file_path):
        """ Convert given file to required format with FFMPEG and process with WADA."""
        _, sr = sf.read(file_path)
        print("Current work dir")
        os.getcwd()
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

    def fit_and_move(self, input_file_dir, metadata_file_name, threshold, output_file_dir,audio_id):

        local_dict = self.fit(input_file_dir)
        clean_dir = output_file_dir + '/clean'
        rejected_dir = output_file_dir + '/rejected'
        metadata = pd.read_csv(metadata_file_name)
        clean_audio_duration=[]
        list_file_utterances_with_duration=[]

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
                y, sr = librosa.load(key)
                clip_duration = librosa.get_duration(y)
                clean_audio_duration.append(clip_duration)
                command = f'mv "{key}" "{clean_dir_local}"'
                command_text = f'mv "{text_file_name_key}" "{clean_dir_local_text}"'
                list_file_utterances_with_duration.append(audio_file_name + ":" + str(clip_duration))
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

            metadata["cleaned_duration"] = math.floor(sum(clean_audio_duration) / 60)
            metadata["audio_id"] = audio_id
            metadata['utterances_files_list'] = str(list_file_utterances_with_duration)
            metadata.to_csv(metadata_file_name,index=False)
            os.system(command + ';' + command_text)

