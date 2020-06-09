from .audio_clipper import AudioClipper
from .srt_generator import SRTGenerator
from .snr import SNR
import os
import time
import yaml
yaml.warnings({'YAMLLoadWarning': False})
import glob, sys
from .gcs_operations import CloudStorageOperations
from .metadata_uploader import CloudSQLUploader
from sqlalchemy import create_engine, select, MetaData, Table, text
from dotenv import load_dotenv
from os.path import join, dirname


class AudioPipeline():
    def __init__(self):
        pass

    def __load_yaml_file(self, path):
        read_dict = {}
        with open(path, 'r') as file:
            read_dict = yaml.load(file)
        return read_dict

    def download_input_blob(self, bucket_name, args_downloader, local_download_path):
        current_working_directory = os.getcwd()
        obj_gcsops.download_to_local(bucket_name=bucket_name,
                                     source_blob_name=os.path.join(args_downloader['tobeprocessed_input_basepath'],
                                                                   data_source,
                                                                   audio_id),
                                     destination=os.path.join(current_working_directory, local_download_path),
                                     is_directory=True)

    def generate_srt_file_single(self, obj_srt, srtgenerator_dict, local_download_path, bucket_name, audio_extn):
        srt_path = obj_srt.fit_single(bin_size=srtgenerator_dict['bin_size'],
                                      input_file_path=local_download_path,
                                      bucket_name=bucket_name,
                                      audio_extn=audio_extn,
                                      output_file_path=srtgenerator_dict['output_file_path'],
                                      dump_response=srtgenerator_dict['dump_response'],
                                      dump_response_directory=srtgenerator_dict['dump_response_directory'],
                                      )
        return srt_path

    def clip_audio_single(self, obj_clip_audio, current_working_directory, srt_path, args_clipaudio,
                          data_source, audio_id):
        clipped_files_dir, metadata_file_name = obj_clip_audio.fit_single(
            srt_file_path=os.path.join(current_working_directory,
                                       srt_path),
            audio_file_path=os.path.join(current_working_directory,
                                         srt_path.split(".")[0] + ".wav"),
            output_file_dir=os.path.join(current_working_directory,
                                         args_clipaudio['output_file_dir'],
                                         data_source,
                                         audio_id))
        return clipped_files_dir, metadata_file_name

    def generate_srt_file_dir(self, obj_srt, bucket_name, srtgenerator_dict, local_download_path, audio_extn):
        srt_path = obj_srt.fit_dir(bin_size=srtgenerator_dict['bin_size'],
                                   bucket_name=bucket_name,
                                   input_file_dir=local_download_path,
                                   audio_extn=audio_extn,
                                   output_file_dir=srtgenerator_dict['output_file_path'],
                                   dump_response=srtgenerator_dict['dump_response'],
                                   dump_response_directory=srtgenerator_dict['dump_response_directory'])
        return srt_path

    def fetch_local_data(self, obj_gcsops, args_downloader, local_destination_path,audio_extn):
        # obj_gcsops.make_directories(local_destination_path)
        local_src_path = args_downloader['local_data_path']
        obj_gcsops.copy_all_files(local_src_path, local_destination_path,audio_extn)

    def fit(self, yaml_file_path, bucket_name, data_source, audio_id, audio_extn, job_mode):
        pipeline_start_time = time.time()
        read_dict = self.__load_yaml_file(yaml_file_path)
        current_working_directory = os.getcwd()

        args_application = read_dict['application']
        args_downloader = read_dict['downloader']
        args_srtgenerator = read_dict['srtgenerator']
        args_clipaudio = read_dict['clipaudio']
        args_snr = read_dict['filtersnr']
        args_metadatadb = read_dict['metadatadb']

        # Create required objects
        obj_srt = SRTGenerator(args_application['language'])
        obj_clip_audio = AudioClipper()
        clipped_files_dir = []

        # Process according to single video or a list of videos
        # Download the to be processed file to local from GCS
        # Convert file to wav 16khz mono channel format
        # Convert audio to text by calling speech to text api
        # Generate subtitle srt file

        obj_gcsops = CloudStorageOperations()
        if (job_mode == "cluster"):
            local_download_path = os.path.join(args_downloader['tobeprocessed_input_basepath'],
                                               data_source,
                                               audio_id)
            print("Initiating raw file download from cloud storage on to local...")
            self.download_input_blob(bucket_name, args_downloader, local_download_path)

            print("Initiating srt file generation process...")
            srt_paths, wav_paths = self.generate_srt_file_single(obj_srt, args_srtgenerator, local_download_path,
                                                                 bucket_name, audio_extn)

            print("Initiating clipping of audio and srt file process...")
            clipped_files_dir, metadata_file_name = self.clip_audio_single(obj_clip_audio, current_working_directory,
                                                                           srt_paths,
                                                                           args_clipaudio,
                                                                           data_source, audio_id)

        else:
            local_download_path = os.path.join(args_downloader['tobeprocessed_input_basepath'],
                                               data_source)
            self.fetch_local_data(obj_gcsops, args_downloader, local_download_path,audio_extn)

            srt_paths, wav_paths = self.generate_srt_file_dir(obj_srt, bucket_name, args_srtgenerator,
                                                              local_download_path, audio_extn)
            # audio_file_path = os.path.join(current_working_directory,
            #                                srt_path.split(".")[0] + ".wav"),
            print("********srt_path******", srt_paths)
            print("********wav_paths******", wav_paths)

        for str_path,wav_path in zip(srt_paths,wav_paths):
            if (job_mode != "cluster"):
                # output_dir = args_clipaudio['output_file_dir'] + '/' + data_source
                # files_written, meta_files_written = obj_clip_audio.fit_dir(srt_dir=srt_paths,
                #                                                            audio_dir=wav_paths,
                #                                                            output_dir=output_dir)

                audio_id=str_path.split('/')[-2]

                print("Initiating clipping of audio and srt file process...")
                clipped_files_dir, metadata_file_name = self.clip_audio_single(obj_clip_audio, current_working_directory,
                                                                               str_path,
                                                                               args_clipaudio,
                                                                               data_source, audio_id)

                print("****clipped_files_dir****", clipped_files_dir)
                print("****metadata_file_name****", metadata_file_name)


            print("Initiating snr process...")
            snr_obj = SNR()
            if args_snr['input_file_dir'] is not None:
                args_snr['input_file_dir'] = glob.glob(args_snr['input_file_dir'] + '/*.wav')
            else:
                args_snr['input_file_dir'] = clipped_files_dir

            snr_obj.fit_and_move(input_file_dir=args_snr['input_file_dir'],
                                 metadata_file_name=metadata_file_name,
                                 output_file_dir=os.path.join(current_working_directory,
                                                              args_clipaudio['output_file_dir'],
                                                              data_source,
                                                              audio_id),
                                 threshold=args_snr['threshold'],
                                 audio_id=audio_id)

            # Upload files to GCS
            # Upload cleaned files
            if (job_mode == "cluster"):
                print("Uploading the cleaned files to cloud storage...")
                obj_gcsops.upload_to_gcs(bucket_name,
                                         os.path.join(current_working_directory,
                                                      args_clipaudio['output_file_dir'],
                                                      data_source,
                                                      audio_id,
                                                      "clean"),
                                         os.path.join(args_clipaudio['output_file_dir'],
                                                      data_source,
                                                      audio_id,
                                                      "clean"),
                                         is_directory=True)

                # # Upload rejected files
                print("Uploading the rejected files to cloud storage...")
                obj_gcsops.upload_to_gcs(bucket_name,
                                         os.path.join(current_working_directory,
                                                      args_clipaudio['output_file_dir'],
                                                      data_source,
                                                      audio_id,
                                                      "rejected"),
                                         os.path.join(args_clipaudio['output_file_dir'],
                                                      data_source,
                                                      audio_id,
                                                      "rejected"),
                                         is_directory=True)

                # # Upload metadata file first to CloudSQL DB and then to GCS
                # # metadata_file_name="/Users/gauravgupta/Documents/Gaurav/Projects/python-dev/26052020_131100_joshtalks_pl_0.csv"
                print("Metadata file name {}".format(metadata_file_name))
                # print("Uploading the metadata file to Cloud SQL DB...")
                # 
                # db_user = args_metadatadb['db_user']
                # db_password = args_metadatadb['db_password']
                # db_name = args_metadatadb['db_name']
                # cloud_sql_connection_name = args_metadatadb['cloud_sql_conn']
                # 
                # db = create_engine(f'postgresql://{db_user}:{db_password}@{cloud_sql_connection_name}/{db_name}')
                # 
                # obj_cloudsql = CloudSQLUploader()
                # obj_cloudsql.upload_file(metadata_file_name, db)
                # print("Metadata file uploaded to Cloud SQL DB successfully")

                print("Uploading the metadata file to cloud storage...")
                obj_gcsops.upload_to_gcs(bucket_name,
                                         metadata_file_name,
                                         os.path.join(args_clipaudio['output_file_dir'],
                                                      "metadata",
                                                      data_source,
                                                      metadata_file_name.split('/')[-1].split('.')[0] + ".csv"),
                                         is_directory=False)


        pipeline_end_time = time.time()
        print("Pipeline took ", pipeline_end_time - pipeline_start_time, " seconds to run!")


if __name__ == "__main__":

    # Get Arguments
    print("Fetching Arguments...")

    if len(sys.argv) > 6:
        job_mode = sys.argv[1]  # local,cluster
        gcs_bucket_name = sys.argv[2]  # remote_gcs bucket name
        config_path = sys.argv[3]  # remote gcs path, for local it will be src/resources/local/config_local.yaml
        data_source = sys.argv[4]  # audio source: joshtalks, brahmakumari
        audio_id = sys.argv[5]  # unique identifier for each audio file
        audio_extn = sys.argv[6]  # audio file exten: can be .mp4 or .mp3
    else:
        print("Required Arguements are not passed correctly.Please retry.")
        exit()

    print("Arguments Received...")
    print("Arg 1 : job_mode : {}".format(job_mode))
    print("Arg 2 : gcs_bucket_name: {}".format(gcs_bucket_name))
    print("Arg 3 : config_path: {}".format(config_path))
    print("Arg 4 : data_source: {}".format(data_source))
    print("Arg 5 : audio_id: {}".format(audio_id))
    print("Arg 6 : audio_extn: {}".format(audio_extn))

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

    obj = AudioPipeline()
    obj.fit(config_local_path, gcs_bucket_name, data_source, audio_id, audio_extn, job_mode=job_mode)
