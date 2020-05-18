import subprocess
from moviepy.audio.AudioClip import AudioClip
from scipy.io import wavfile
from moviepy.audio.io.AudioFileClip import AudioFileClip
import glob


class Data:
    def __init__(self):
        self.start_time = ''
        self.end_time = ''
        self.text = ''

    def print_fn(self):
        print(" Start Time: ", self.start_time, " End Time: ", self.end_time)
        print(self.text)
        print("*"*30)


class ClipAudio(object):
    def __init__(self):
        pass

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
                obj.start_time = arr[0]
                obj.end_time = arr[2]
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

        clip = AudioFileClip(filename = audio_file_path)
        
        for index, obj in enumerate(list_obj):
            new_file_name = output_file_dir + '/' + str(index) + '_' + audio_file_path.split('/')[-1].split('.')[0]

            newclip = clip.subclip(obj.start_time,obj.end_time)

            newclip.write_audiofile( new_file_name + '.wav' )


            with open(new_file_name + '.txt', 'w', encoding='utf8') as file:
                file.write(obj.text)

    def fit_single(self, srt_file_path, audio_file_path, output_file_dir):
        list_objs = self.preprocess_srt(srt_file_path)
        self.clip_audio_with_ffmeg(list_objs, audio_file_path, output_file_dir)

    def fit_dir(self, srt_dir, audio_dir, output_dir):
        srt_filenames = glob.glob(srt_dir + '/*.srt')
        audio_filenames = glob.glob(audio_dir + '/*.wav')

        srt_filenames.sort()
        audio_filenames.sort()

        for audio_, srt_ in zip(audio_filenames, srt_filenames):
            self.fit_single(srt_, audio_, output_dir)



# list_objs = clip_audio_preprocess(r'./data_demo/subtitles_new.srt', 
#             r'./data_demo/1.wav')

# clip_audio_with_ffmeg(list_objs, './data_demo/1.wav')


