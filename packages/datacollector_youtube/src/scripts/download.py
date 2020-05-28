from pytube import Playlist
import pytube
from tqdm import tqdm
import subprocess
import glob
from datetime import datetime
import yaml
import uuid
import csv
import sys
import os
from .gcs_operations import CloudStorageOperations

yaml.warnings({'YAMLLoadWarning': False})


class DownloadVideo(object):
    def __init__(self, extension='mp4'):
        self.audio_path = []
        self.extension = extension

    def __load_yaml_file(self, path):
        read_dict = {}
        with open(path, 'r') as file:
            read_dict = yaml.load(file)
        return read_dict

    def make_directories(self,path):
        if not os.path.exists(path):
            os.makedirs(path)
            print("Directory {} created successfully".format(path))
        else:
            print("Directory {} already exists".format(path))

    def download_data(self,yaml_file_path,current_work_dir):
        read_dict = self.__load_yaml_file(yaml_file_path)
        args_downloader = read_dict['downloader']
        mode = args_downloader["mode"]
        link = args_downloader["link"]
        output_dir_path = args_downloader["output_dir_path"]
        filename_prefix = args_downloader["filename_prefix"]
        source = args_downloader["source"]
        output_full_path = os.path.join(current_work_dir,output_dir_path,source)

        # Create local directory to download data if not exists
        self.make_directories(output_full_path)
        self.fit(link, output_full_path, mode,filename_prefix,yaml_file_path)

    def upload_data_to_gcs(self,yaml_file_path,current_work_dir):
        # Upload the downloaded data to GCS
        print("Initiating data upload to cloud storage")

        read_dict = self.__load_yaml_file(yaml_file_path)
        args_downloader = read_dict['downloader']
        source_dir_path = args_downloader["output_dir_path"]
        source = args_downloader["source"]

        source_full_path = os.path.join(current_work_dir, source_dir_path,source)
        obj_gcsops = CloudStorageOperations()
        obj_gcsops.upload_to_gcs(bucket_name=gcs_bucket_name,
                                source=source_full_path,
                                destination_blob_name=os.path.join(source_dir_path,source),
                                is_directory=True)
        print("Upload to cloud storage completed")


    def create_metadata(self, yaml_file_path):
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

    def download_playlist(self, playlist_url, output_path, filename_prefix,yaml_file_path):
        playlist = Playlist(playlist_url)
        videos_downloaded = []

        for index, link in tqdm(enumerate(playlist)):
            print("Downloading Video: ", index)
            audio_path = self.download_video(video_url=link, output_path=output_path,
                                            filename_prefix=filename_prefix + '_pl_' + str(index),yaml_file_path=yaml_file_path)

            print("Downloaded video: {} successsfully".format(audio_path))
            videos_downloaded.append(audio_path)

        self.audio_path = videos_downloaded
        return videos_downloaded

    def download_video(self, video_url, output_path, filename_prefix,yaml_file_path):

        name = self.generate_unique_filename(filename_prefix)
        filename = name + '.' + self.extension
        metadata_file_name = name + '.' + "csv"
        metadata = self.create_metadata(yaml_file_path=yaml_file_path)

        try:
            yt = pytube.YouTube(video_url)
            metadata['duration'] = yt.length / 60
            metadata['raw_file_name'] = yt.title
            metadata['source_url']=video_url

            print("Downloading video: {}".format(name))
            yt.streams.filter(only_audio=True)[0].download(output_path=output_path, filename=str(name))
            print("Downloaded video: {} successfully".format(name))
            file_path = output_path + '/' + filename

            print("Generating metadata file...")
            metadata_file_path = output_path + '/' + metadata_file_name
            with open(metadata_file_path, 'w') as f:
                w = csv.DictWriter(f, metadata.keys())
                w.writeheader()
                w.writerow(metadata)
            print ("Metadata file generated successfully at: {} path".format(metadata_file_path))
            return file_path
        except:
            print("Connection error at video ", video_url)
            return None

    def generate_unique_filename(self, filename_prefix=None):
        date_part = datetime.today().strftime('%d%m%Y_%H%M%S')

        if filename_prefix is None:
            file_name_for_saving = str(uuid.uuid4())
        else:
            file_name_for_saving = filename_prefix

        return date_part + '_' + file_name_for_saving


    def fit(self, link, output_dir_path, mode, filename_prefix, yaml_file_path):
        ## modes present video, playlist, videolist

        if mode == 'video':
            path = self.download_video(link, output_dir_path, filename_prefix,yaml_file_path)
            print('path received is ', path)
            return path

        if mode == 'playlist':
            list_paths = self.download_playlist(link, output_dir_path, filename_prefix,yaml_file_path)
            return list_paths

        if mode == 'videolist':
            list_paths = []
            read_dict = self.__load_yaml_file(yaml_file_path)
            args_downloader = read_dict['downloader']
            link = args_downloader["link"]
            videolist = link.split(",")
            for index, link in enumerate(videolist):

                path = ''
                if filename_prefix is not None:
                    new_prefix = filename_prefix + '_' + index
                    path = self.download_video(link, output_dir_path, new_prefix,yaml_file_path)

                path = self.download_video(link, output_dir_path, filename_prefix,yaml_file_path)
                list_paths.append(path)

            return list_paths
        print('Finished Downloading')


if __name__ == "__main__":
    # Get Arguments
    job_mode = sys.argv[1]  # local,cluster
    gcs_bucket_name = sys.argv[2]  # remote_gcs bucket name
    config_path = sys.argv[3]  # remote gcs path, for local it will be src/resources/local/config.yaml

    current_working_directory = os.getcwd()
    config_local_path = os.path.join(current_working_directory, "src/resources/" + job_mode + "/config.yaml")

    if (job_mode == "cluster"):
        # Download config file from GCS
        print("Downloading config file from cloud storage to local")
        obj_gcs = CloudStorageOperations()
        obj_gcs.download_to_local(bucket_name=gcs_bucket_name,
                                  source_blob_name=config_path,
                                  destination=config_local_path,
                                  is_directory=False)

    # Download videos
    obj = DownloadVideo()
    obj.download_data(yaml_file_path=config_local_path,current_work_dir=current_working_directory)

    obj.upload_data_to_gcs(yaml_file_path=config_local_path,current_work_dir=current_working_directory)
