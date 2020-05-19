from pytube import Playlist
import pytube
import os
from tqdm import tqdm
import subprocess
import glob
from datetime import datetime

import uuid

class DownloadVideo(object):
    def __init__(self, extension='mp4'):
        self.audio_path = []
        self.extension = extension

    def download_playlist(self, playlist_url, output_path, filename_prefix):
        playlist= Playlist(playlist_url)

        videos_downloaded = []
        
        for index, link in tqdm(enumerate(playlist)):
            print("Downloading Video: ", index)

            audio_path = self.download_video(video_url = link, output_path = output_path, filename_prefix = filename_prefix + '_pl_'+str(index) )

            videos_downloaded.append(audio_path)

        self.audio_path = videos_downloaded
        return videos_downloaded

    def download_video(self, video_url, output_path, filename_prefix):
        
        name = self.generate_unique_filename(filename_prefix)
        filename = name + '.' + self.extension


        try:
            yt = pytube.YouTube(video_url)
            yt.streams.filter(only_audio=True)[0].download(output_path = output_path, filename = str(name))
            return output_path + '/' + filename
        except:
            print("Connection error at video ", video_url)
            return None

    def generate_unique_filename(self, filename_prefix):
        date_part = datetime.today().strftime('%d%m%Y_%H%M%S')

        if filename_prefix is None:
            file_name_for_saving = str(uuid.uuid4())
        else: 
            file_name_for_saving = filename_prefix
        
        return date_part + '_' + file_name_for_saving



    def convert_to_wav(self, output_dir = None, list_paths = None):
        if list_paths is not None:
            self.audio_path = list_paths

        output_file_paths = []
        for file in self.audio_path:
            print(file)
            
            input_file_name = file
            
            output_file_name = file.split('/')[-1].split('.')[0] + '.wav'
            
            if output_dir is None:
                output_file_path = "/".join(file.split('/')[:-1]) + '/' + output_file_name
            else:
                output_file_path = output_dir + '/' + output_file_name


            command = f"ffmpeg -i {input_file_name} -ar 16000 -ac 1 -bits_per_raw_sample 16 {output_file_path}"
            subprocess.call(command, shell=True)

            output_file_paths.append(output_file_path)
        
        return output_file_paths

        

    def fit(self, link,  output_dir_path, mode='video',filename_prefix = None, convert_to_wav = True, output_wav_dir = None):
        ## modes present video, playlist, videolist

        if mode == 'video':
            path = self.download_video(link, output_dir_path, filename_prefix)

            print('path received is ', path)
            
            if convert_to_wav:
                return self.convert_to_wav(output_dir= output_wav_dir, list_paths=[path])
                
            return path


        if mode == 'playlist':
            list_paths = self.download_playlist(link, output_dir_path, filename_prefix)

            if convert_to_wav:
                return self.convert_to_wav(output_dir= output_wav_dir, list_paths=list_paths)
            return list_paths

        if mode == 'videolist':
            list_paths = []

            for index, link in enumerate(videolist):
                
                path = ''
                if filename_prefix is not None:
                    new_prefix = filename_prefix + '_' + strindex
                    path = self.download_video(link, output_dir_path, new_prefix)

                path = self.download_video(link, output_dir_path, filename_prefix)
                list_paths.append(path)


            
            if convert_to_wav:
                return self.convert_to_wav(output_dir= output_wav_dir, list_paths=list_paths)

            return list_paths
        print('Finished Downloading')





if __name__ == "__main__":
    dwn = DownloadVideo()
    #dwn.download_playlist('https://www.youtube.com/playlist?list=PL1D6nWQpbEZdtH4G1TZNrBVj1zBfTOhA7', './')
    dwn.download_video('https://www.youtube.com/watch?v=TMoCrV3D1kU&feature=youtu.be', './data_demo', '1')

    # list_dir = glob.glob(r'..//data/*.mp4')
    # print(list_dir)
    dwn.convert_to_wav(list_paths = ['./data_demo/1.mp4'])