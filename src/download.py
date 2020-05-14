from pytube import Playlist
import pytube
import os
from tqdm import tqdm
import subprocess
import glob

class DownloadVideo(object):
    def __init__(self):
        self.audio_path = []

    def download_playlist(self, playlist_url, output_path):
        playlist= Playlist(playlist_url)
        print(playlist)

        videos_downloaded = []
        
        for i, link in tqdm(enumerate(playlist)):
            print("Downloading Video: ", i)
            audio_path = self.download_video(video_url = link, output_path = output_path, filename = i)

            videos_downloaded.append(audio_path)

        self.audio_path = videos_downloaded

    def download_video(self, video_url, output_path, filename):
        try:
            yt = pytube.YouTube(video_url)
            yt.streams.filter(only_audio=True)[0].download(output_path = output_path, filename = str(filename))
            return output_path + './' + filename
        except:
            print("Connection error at video ", video_url)

        #print("filename is ", filename)


    def convert_to_wav(self, list_paths = None):
        if list_paths is not None:
            self.audio_path = list_paths

        for file in self.audio_path:

            input_file_name = file
            output_file_name = file.split('/')[-1].split('.')[0] + '.wav'
            output_file_path = file.split('/')[:-1] + '/' + output_file_name

            command = f'ffmpeg -i {input_file_name} -ar 44100 -ac 1 -bits_per_raw_sample 16 {output_file_name}'
            subprocess.call(command, shell=True)
        

        



if __name__ == "__main__":
    dwn = DownloadVideo()
    #dwn.download_playlist('https://www.youtube.com/playlist?list=PL1D6nWQpbEZdtH4G1TZNrBVj1zBfTOhA7', './')
    list_dir = glob.glob('/home/harveen.chadha/gcs_mnt/data/audiotospeech/raw/landing/hindi/audio/joshtalks/*.mp4')

    dwn.convert_to_wav(list_dir)