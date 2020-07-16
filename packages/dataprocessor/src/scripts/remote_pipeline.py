import glob
import os
import time

import yaml

from .snr import SNR
from .transcription_generator import create_transcription
from .vad_audio_clipper import create_audio_clips
from .google_speech_client import GoogleSpeechClient
from .merge_chunks import merge_chunks
from .wav_convertor import convert_to_wav

yaml.warnings({'YAMLLoadWarning': False})
from .gcs_operations import CloudStorageOperations
from .metadata_uploader import CloudSQLUploader
from sqlalchemy import create_engine


class RemoteAudioPipeline():
    def __init__(self):
        pass

    def __load_yaml_file(self, path):
        print('loading config file...')
        read_dict = {}
        with open(path, 'r') as file:
            read_dict = yaml.load(file)
        print('loading config file done...')
        return read_dict

    def download_input_blob(self, obj_gcsops, bucket_name, args_downloader, local_download_path, data_source, audio_id):
        current_working_directory = os.getcwd()
        obj_gcsops.download_to_local(bucket_name=bucket_name,
                                     source_blob_name=os.path.join(args_downloader['tobeprocessed_input_basepath'],
                                                                   data_source,
                                                                   audio_id),
                                     destination=os.path.join(current_working_directory, local_download_path),
                                     is_directory=True)

    def fetch_local_data(self, obj_gcsops, args_downloader, local_destination_path, audio_extn):
        # obj_gcsops.make_directories(local_destination_path)
        local_src_path = args_downloader['local_data_path']
        obj_gcsops.copy_all_files(local_src_path, local_destination_path, audio_extn)

    def fit(self, yaml_file_path, bucket_name, data_source, audio_id, audio_extn):
        print("starting RemoteAudioPipeline.........")
        pipeline_start_time = time.time()
        read_dict = self.__load_yaml_file(yaml_file_path)
        args_application = read_dict['application']
        args_downloader = read_dict['downloader']
        args_srtgenerator = read_dict['srtgenerator']
        args_clipaudio = read_dict['clipaudio']
        args_snr = read_dict['filtersnr']
        args_metadatadb = read_dict['metadatadb']

        # Create required objects
        google_speec_client = GoogleSpeechClient(args_application['language'])

        obj_gcsops = CloudStorageOperations()

        local_download_path = os.path.join(args_downloader['tobeprocessed_input_basepath'],
                                           data_source,
                                           audio_id)
        print(f'Initiating raw file download from cloud storage on to local folder:{local_download_path}')
        self.download_input_blob(obj_gcsops, bucket_name, args_downloader, local_download_path, data_source, audio_id)
        output_file_path = args_srtgenerator['output_file_path']
        current_working_directory = os.getcwd()
        if output_file_path is None:
            converted_wav_file_path = convert_to_wav(os.path.join(current_working_directory, local_download_path),
                                                     output_file_path, audio_extn)

        else:
            converted_wav_file_path = convert_to_wav(os.path.join(current_working_directory, local_download_path),
                                                     os.path.join(current_working_directory, output_file_path),
                                                     audio_extn)

        chunks_dir = os.path.join(local_download_path, 'chunks')
        vad_output_path = local_download_path + 'vad_output.txt'
        obj_gcsops.make_directories(chunks_dir)

        print(f'******** creating chunks using vad for file:{converted_wav_file_path} to folder {chunks_dir}')
        create_audio_clips(2, converted_wav_file_path, chunks_dir, vad_output_path)

        print("******** merging chunks in :", chunks_dir)
        voice_separator_audio = './src/resources/chunk_separator/hoppi.wav'
        merged_file_name = merge_chunks(chunks_dir, voice_separator_audio, 'chunk', 'merged.wav')
        remote_wav_file_path = os.path.join(local_download_path, merged_file_name)

        print(f'******** uploading merged file:{merged_file_name} --> {remote_wav_file_path}')

        obj_gcsops.upload_to_gcs(bucket_name, merged_file_name, remote_wav_file_path, False)

        rejected_chunks = create_transcription(google_speec_client, remote_wav_file_path, chunks_dir)
        print('chunks rejected due to <<NO TRANSCRIPTIONS>>' + str(rejected_chunks))
        metadata_file_name = converted_wav_file_path.replace('.wav', '.csv')

        print("Initiating snr process...")
        snr_obj = SNR()
        args_snr['input_file_dir'] = glob.glob(chunks_dir + '/*.wav')
        print("*****SNR input dir*******", args_snr['input_file_dir'])
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

        self.upload_metadata(args_clipaudio, args_metadatadb, bucket_name, data_source, metadata_file_name, obj_gcsops)

        pipeline_end_time = time.time()
        print("Pipeline took ", pipeline_end_time - pipeline_start_time, " seconds to run!")

    def upload_metadata(self, args_clipaudio, args_metadatadb, bucket_name, data_source, metadata_file_name,
                        obj_gcsops):
        # # Upload metadata file first to CloudSQL DB and then to GCS
        print("Metadata file name {}".format(metadata_file_name))
        print("Uploading the metadata file to Cloud SQL DB...")
        db_user = args_metadatadb['db_user']
        db_password = args_metadatadb['db_password']
        db_name = args_metadatadb['db_name']
        cloud_sql_connection_name = args_metadatadb['cloud_sql_conn']
        db = create_engine(f'postgresql://{db_user}:{db_password}@{cloud_sql_connection_name}/{db_name}')
        obj_cloudsql = CloudSQLUploader()
        obj_cloudsql.upload_file(metadata_file_name, db)
        print("Metadata file uploaded to Cloud SQL DB successfully")
        print("Uploading the metadata file to cloud storage...")
        obj_gcsops.upload_to_gcs(bucket_name,
                                 metadata_file_name,
                                 os.path.join(args_clipaudio['output_file_dir'],
                                              "metadata",
                                              data_source,
                                              ),
                                 is_directory=False)
