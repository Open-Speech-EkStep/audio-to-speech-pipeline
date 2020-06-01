import subprocess
from moviepy.audio.AudioClip import AudioClip
from scipy.io import wavfile
from moviepy.audio.io.AudioFileClip import AudioFileClip
import glob
import os
from pydub import AudioSegment
from datetime import datetime
import time
import pandas as pd 


class Data:
    def __init__(self):
        self.start_time = ''
        self.end_time = ''
        self.text = ''

    def print_fn(self):
        print(" Start Time: ", self.start_time, " End Time: ", self.end_time)
        print(self.text)
        print("*"*30)


class AudioClipper(object):
    def __init__(self):
        pass

    def make_directories(self,path):
        if not os.path.exists(path):
            os.makedirs(path)
            print("Directory {} created successfully".format(path))
            return 0
        else:
            print("Directory {} already exists".format(path))
            return 1

    def preprocess_srt(self, srt_file_path):
        lines = []
        with open(srt_file_path, "r", encoding='utf-8') as file:
            lines = file.readlines()
        
        lines_mapping = []

        l = len(lines)
        for index, line in enumerate(lines):

            if '-->' in line:
                arr = line.split(' ')
                obj = Data()
                
                #print(arr[0])
                start = datetime.strptime(arr[0], '%H:%M:%S,%f')
                timedelta = start - datetime(1900,1,1)

                obj.start_time = timedelta.total_seconds() * 1000 
                
                #print(obj.start_time)
                
                if(arr[2]) != "":
                    #print(arr[2])
                    end = datetime.strptime(arr[2].strip(), '%H:%M:%S,%f')
                    timedelta = end - datetime(1900,1,1)

                    obj.end_time =  timedelta.total_seconds() * 1000
                else:
                    obj.end_time = 0
                #print(obj.end_time)
                if index < (l-1):
                    next_ = lines[index + 1]
                    obj.text = next_
                lines_mapping.append(obj)

                index = index + 1
        
        # for obj in lines_mapping:
        #     obj.print_fn()

        return lines_mapping


    
    def clip_audio_with_ffmeg(self, list_obj, audio_file_path, output_file_dir):
        
        new_file_path = '/'.join( audio_file_path.split('/')[:-1] )
        metadata_file_path = new_file_path + "/" + audio_file_path.split('/')[-1].split('.')[0] + ".csv"
        metadata = pd.read_csv(metadata_file_path)

        sound = AudioSegment.from_wav(audio_file_path)

        files_written = []
        list_audio_utterances = []

        for index, obj in enumerate(list_obj):

            if not os.path.exists(output_file_dir):
                self.make_directories(output_file_dir)

            new_file_name = output_file_dir + '/' + str(index) + '_' + audio_file_path.split('/')[-1].split('.')[0]
            newclip =sound[ obj.start_time: obj.end_time]
            newclip.export(new_file_name + '.wav', format='wav')
            
            files_written.append(new_file_name + '.wav')

            #Generate list of utterances to be updated in metadata file
            list_audio_utterances.append(str(index) + '_' + audio_file_path.split('/')[-1].split('.')[0] + '.wav')

            with open(new_file_name + '.txt', 'w', encoding='utf8') as file:
                file.write(obj.text.strip())

        # metadata['utterances_file_list'] = str(list_audio_utterances)
        metadata_file_name = os.path.join(output_file_dir, audio_file_path.split('/')[-1].split('.')[0] + ".csv")
        metadata.to_csv(metadata_file_name,index=False)

        return files_written, metadata_file_name


    def fit_single(self, srt_file_path, audio_file_path, output_file_dir):
        list_objs = self.preprocess_srt(srt_file_path)
        return self.clip_audio_with_ffmeg(list_objs, audio_file_path, output_file_dir)

    def fit_dir(self, srt_dir, audio_dir, output_dir):

        srt_dir.sort()
        audio_dir.sort()
        files_written = []

        for audio_, srt_ in zip(audio_dir, srt_dir):
            local_written = self.fit_single(srt_, audio_, output_dir)
            files_written.extend(local_written)
        return files_written



