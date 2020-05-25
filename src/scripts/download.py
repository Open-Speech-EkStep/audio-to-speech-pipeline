from pytube import Playlist
import pytube
from tqdm import tqdm
import subprocess
import glob
from datetime import datetime
import yaml
import uuid
import csv

yaml.warnings({'YAMLLoadWarning': False})

YAML_FILE_PATH = 'config.yaml'


class DownloadVideo(object):
    def __init__(self, extension='mp4'):
        self.audio_path = []
        self.extension = extension

    def __load_yaml_file(self, path):
        read_dict = {}
        with open(path, 'r') as file:
            read_dict = yaml.load(file)
        return read_dict

    def create_metadata(self, yaml_file_path=YAML_FILE_PATH):
        read_dict = self.__load_yaml_file(yaml_file_path)
        args_downloader = read_dict['downloader']
        metadata = {'raw_file_name': args_downloader['raw_file_name'],
                    'duration': args_downloader['duration'],
                    'title': args_downloader['title'],
                    'speaker_name': args_downloader['speaker_name'],
                    'audio_id': args_downloader['audio_id'],
                    'cleaned_duration': args_downloader['cleaned_duration'],
                    'num_of_speakers': args_downloader['num_of_speakers'],
                    'language': args_downloader['language'],
                    'has_other_audio_signature': args_downloader['has_other_audio_signature'],
                    'type': args_downloader['type'],
                    'source': args_downloader['source'],
                    'experiment_use': args_downloader['experiment_use'],
                    'utterances_file_list': args_downloader['utterances_file_list'],
                    'source_url': args_downloader['source_url'],
                    'speaker_gender': args_downloader['speaker_gender'],
                    'source_website': args_downloader['source_website'],
                    'experiment_name': args_downloader['experiment_name'],
                    'mother_tongue': args_downloader['mother_tongue'],
                    'age_group': args_downloader['age_group'],
                    'recorded_state': args_downloader['recorded_state'],
                    'recorded_district': args_downloader['recorded_district'],
                    'recorded_place': args_downloader['recorded_place'],
                    'recorded_date': args_downloader['recorded_date'],
                    'purpose': args_downloader['purpose']}
        return metadata

    def download_playlist(self, playlist_url, output_path, filename_prefix):
        playlist = Playlist(playlist_url)

        videos_downloaded = []
        videos_metadata = []

        for index, link in tqdm(enumerate(playlist)):
            print("Downloading Video: ", index)

            audio_path, audio_metadata = self.download_video(video_url=link, output_path=output_path,
                                                             filename_prefix=filename_prefix + '_pl_' + str(index))
            videos_downloaded.append(audio_path)
            videos_metadata.append(audio_metadata)

        self.audio_path = videos_downloaded
        return videos_downloaded, videos_metadata

    def download_video(self, video_url, output_path, filename_prefix):

        name = self.generate_unique_filename(filename_prefix)
        filename = name + '.' + self.extension
        metadata_file_name = name + '.' + "csv"
        metadata = self.create_metadata(yaml_file_path=YAML_FILE_PATH)

        try:
            yt = pytube.YouTube(video_url)
            metadata['duration'] = yt.length / 60
            metadata['raw_file_name'] = yt.title
            yt.streams.filter(only_audio=True)[0].download(output_path=output_path, filename=str(name))
            file_path = output_path + '/' + filename
            metadata_file_path = output_path + '/' + metadata_file_name
            with open(metadata_file_path, 'w') as f:
                w = csv.DictWriter(f, metadata.keys())
                w.writeheader()
                w.writerow(metadata)
            return file_path, metadata
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

    def convert_to_wav(self, output_dir=None, list_paths=None):
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

    def fit(self, link, output_dir_path, mode='video', filename_prefix=None, convert_to_wav=True, output_wav_dir=None):
        ## modes present video, playlist, videolist

        if mode == 'video':
            path = self.download_video(link, output_dir_path, filename_prefix)

            print('path received is ', path)

            if convert_to_wav:
                return self.convert_to_wav(output_dir=output_wav_dir, list_paths=[path])

            return path

        if mode == 'playlist':
            list_paths = self.download_playlist(link, output_dir_path, filename_prefix)

            if convert_to_wav:
                return self.convert_to_wav(output_dir=output_wav_dir, list_paths=list_paths)
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
                return self.convert_to_wav(output_dir=output_wav_dir, list_paths=list_paths)

            return list_paths
        print('Finished Downloading')


if __name__ == "__main__":
    dwn = DownloadVideo()
    # dwn.download_playlist('https://www.youtube.com/playlist?list=PL1D6nWQpbEZdtH4G1TZNrBVj1zBfTOhA7', './')
    #f, m = dwn.download_video('https://www.youtube.com/watch?v=ZV202pMGkTo', '/home/anirudh/Desktop/youtube-videos', '1')
    f, m = dwn.download_playlist('https://www.youtube.com/watch?v=o4RIZllaRd0&list=PLoEiobi8_gm5tP_kUE9fzCAs7aK0OIIjw', '/home/anirudh/Desktop/youtube-videos','3')
    # yt = pytube.YouTube('https://www.youtube.com/watch?v=TMoCrV3D1kU&feature=youtu.be')
    # t = dwn.create_base_metadata(yt)
    # t = dwn.create_base_metadata('config.yaml')
    #print(f)
    #print(m)
    # list_dir = glob.glob(r'..//data/*.mp4')
    # print(list_dir)
    # dwn.convert_to_wav(list_paths=['./data_demo/1.mp4'])
