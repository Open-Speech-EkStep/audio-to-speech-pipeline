from pytube import Playlist
import pytube
import os
from tqdm import tqdm

class Download(object):
    def __init__(self):
        pass

    def download_playlist(self, playlist_url, output_path):
        playlist= Playlist(playlist_url)
        
        for i,link in tqdm(enumerate(playlist)):
            print("Downloading Video: ", i)
            self.download_video(link, output_path, i)

    def download_video(self, video_url, output_path, filename):
        try:
            yt = pytube.YouTube(video_url)
        except:
            print("Connection error at video ", video_url)

        yt.streams.filter(only_audio=True)[0].download(output_path = output_path, filename=filename)

