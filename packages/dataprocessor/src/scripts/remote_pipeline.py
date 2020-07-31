import glob
import os
import time

import yaml

from .azure_speech_client import AzureSpeechClient
from .google_speech_client import GoogleSpeechClient
from .snr import SNR
from .transcription_generator import create_azure_transcription, create_google_transcription
from .vad_audio_clipper import create_audio_clips
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

    def fit(self, yaml_file_path, bucket_name, data_source, audio_id, audio_extn, stt_api):
        print("starting RemoteAudioPipeline.........")
        pipeline_start_time = time.time()
        read_dict = self.__load_yaml_file(yaml_file_path)
        args_application = read_dict['application']
        args_downloader = read_dict['downloader']
        args_srtgenerator = read_dict['srtgenerator']
        args_clipaudio = read_dict['clipaudio']
        args_snr = read_dict['filtersnr']
        args_metadatadb = read_dict['metadatadb']
        args_azure = read_dict['azure']

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
        base_chunk_name = converted_wav_file_path.split('/')[-1]
        create_audio_clips(2, converted_wav_file_path, chunks_dir, vad_output_path, base_chunk_name)

        metadata_file_name = converted_wav_file_path.replace('.wav', '.csv')

        print("Initiating snr process...")
        snr_obj = SNR()
        args_snr['input_file_dir'] = glob.glob(chunks_dir + '/*.wav')
        print("*****SNR input dir*******", args_snr['input_file_dir'])
        snr_output_base_dir = os.path.join(current_working_directory, args_clipaudio['output_file_dir'], data_source,
                                           audio_id)
        snr_obj.fit_and_move(input_file_dir=args_snr['input_file_dir'],
                             metadata_file_name=metadata_file_name,
                             output_file_dir=snr_output_base_dir,
                             threshold=args_snr['threshold'],
                             audio_id=audio_id)

        transcription_input_dir = os.path.join(snr_output_base_dir, 'clean')
        print(f'******** creating transcriptions for folder {transcription_input_dir}')
        chunk_files = os.listdir(transcription_input_dir)
        for chunk_file_name in chunk_files:
            local_wave_file_path = os.path.join(transcription_input_dir, chunk_file_name)
            self.generate_transcription(args_application, args_azure, bucket_name, chunk_file_name, local_download_path,
                                        local_wave_file_path, obj_gcsops, snr_output_base_dir, stt_api)

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

    def generate_transcription(self, args_application, args_azure, bucket_name, chunk_file_name, local_download_path,
                               local_wave_file_path, obj_gcsops, snr_output_base_dir, stt_api):
        if stt_api == 'azure':
            try:
                create_azure_transcription(AzureSpeechClient(args_azure['speech_key'], args_azure['region'])
                                           , args_application['language'],
                                           local_wave_file_path)
            except RuntimeError:
                print('Azure API call failed..')
                rejected_dir = snr_output_base_dir + '/rejected'
                if not os.path.exists(rejected_dir):
                    os.makedirs(rejected_dir)
                command = f'mv {local_wave_file_path} {rejected_dir}'
                print(f'moving bad wav file: {local_wave_file_path} to rejected folder: {rejected_dir}')
                os.system(command)

        elif stt_api == 'google':
            remote_wav_file_path = os.path.join(local_download_path, 'stt_chunks', chunk_file_name)
            print(f'******** uploading wav file:{local_wave_file_path} --> {remote_wav_file_path}')
            obj_gcsops.upload_to_gcs(bucket_name, local_wave_file_path, remote_wav_file_path, False)

            remote_wav_file_path_for_api = os.path.join("gs://", bucket_name, remote_wav_file_path)
            print(f'******** using wav file:{remote_wav_file_path_for_api} for google API')
            create_google_transcription(GoogleSpeechClient(args_application['language']), remote_wav_file_path_for_api,
                                        local_wave_file_path)
        else:
            raise RuntimeError(f'{stt_api} not configured')

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
