from .audio_clipper import AudioClipper
from .srt_generator import SRTGenerator
from .snr import SNR
import os
import time
import yaml
yaml.warnings({'YAMLLoadWarning': False})
import glob,sys
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
    
    def fit(self, yaml_file_path,bucket_name,data_source,audio_id,audio_extn):
        pipeline_start_time = time.time()
        read_dict = self.__load_yaml_file(yaml_file_path)
        current_working_directory = os.getcwd()

        args_application = read_dict['application']
        args_downloader = read_dict['downloader']
        args_srtgenerator  = read_dict['srtgenerator']
        args_clipaudio  = read_dict['clipaudio']
        args_snr = read_dict['filtersnr']
        args_metadatadb = read_dict['metadatadb']

        # Create required objects
        obj_srt = SRTGenerator(args_application['language'])
        obj_clip_audio = AudioClipper()
        clipped_files_dir = []

        # Process according to single video or a list of videos
        if args_downloader['mode'] == 'video':
            # Download the to be processed file to local from GCS
            # Convert file to wav 16khz mono channel format
            # Convert audio to text by calling speech to text api
            # Generate subtitle srt file

            obj_gcsops = CloudStorageOperations()
            local_download_path = os.path.join(args_downloader['tobeprocessed_input_basepath'],
                                                data_source,
                                                audio_id)

            print("Initiating raw file download from cloud storage on to local...")
            obj_gcsops.download_to_local(bucket_name=bucket_name,
                                         source_blob_name=os.path.join(args_downloader['tobeprocessed_input_basepath'],
                                                                    data_source,
                                                                    audio_id),
                                         destination=os.path.join(current_working_directory,local_download_path),
                                         is_directory=True)

            print("Initiating srt file generation process...")
            srt_path = obj_srt.fit_single(bin_size = args_srtgenerator['bin_size'],
                                        input_file_path = local_download_path,
                                        bucket_name = bucket_name,
                                        audio_extn=audio_extn,
                                        output_file_path= args_srtgenerator['output_file_path'],
                                        dump_response= args_srtgenerator['dump_response'],
                                        dump_response_directory = args_srtgenerator['dump_response_directory'],
                                        )
            print("Initiating clipping of audio and srt file process...")
            clipped_files_dir, metadata_file_name = obj_clip_audio.fit_single(srt_file_path = os.path.join(current_working_directory,
                                                                                   srt_path),
                                    audio_file_path = os.path.join(current_working_directory,
                                                                   srt_path.split(".")[0] + ".wav"),
                                    output_file_dir = os.path.join(current_working_directory,
                                                                   args_clipaudio['output_file_dir'],
                                                                   data_source,
                                                                   audio_id))
        # else:
        #     srt_path = obj_srt.fit_dir(bin_size = args_srtgenerator['bin_size'],
        #                                 input_file_dir = output_file_paths,
        #                                 output_file_dir = args_srtgenerator['output_file_path'],
        #                                 dump_response= args_srtgenerator['dump_response'],
        #                                 dump_response_directory = args_srtgenerator['dump_response_directory'])
        #
        #
        #     files_written = obj_clip_audio.fit_dir(srt_dir = srt_path,
        #                             audio_dir = output_file_paths,
        #                             output_dir = args_clipaudio['output_file_dir'])

        print("Initiating snr process...")
        snr_obj = SNR()
        if args_snr['input_file_dir'] is not None:
            args_snr['input_file_dir'] = glob.glob( args_snr['input_file_dir'] +'/*.wav' )
        else:
            args_snr['input_file_dir']  = clipped_files_dir

        snr_obj.fit_and_move(input_file_dir = args_snr['input_file_dir'],
                            metadata_file_name = metadata_file_name,
                            output_file_dir = os.path.join(current_working_directory,
                                                                   args_clipaudio['output_file_dir'],
                                                                   data_source,
                                                                   audio_id),
                            threshold = args_snr['threshold'],
                            audio_id=audio_id )

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

        # Upload metadata file first to CloudSQL DB and then to GCS
        print("Metadata file name {}".format(metadata_file_name))
        print("Uploading the metadata file to Cloud SQL DB...")

        db_user=args_metadatadb['db_user']
        db_password=args_metadatadb['db_password']
        db_name = args_metadatadb['db_name']
        cloud_sql_connection_name = args_metadatadb['cloud_sql_conn']

        db = create_engine(f'postgresql://{db_user}:{db_password}@{cloud_sql_connection_name}/{db_name}')

        obj_cloudsql = CloudSQLUploader()
        obj_cloudsql.upload_file(metadata_file_name,db)
        print("Metadata file uploaded to Cloud SQL DB successfully")

        print("Uploading the metadata file to cloud storage...")
        obj_gcsops.upload_to_gcs(bucket_name,
                                 metadata_file_name,
                                 os.path.join(args_clipaudio['output_file_dir'],
                                              "metadata",
                                              data_source,
                                              metadata_file_name.split('/')[-1].split('.')[0]+".csv"),
                                 is_directory=False)

        pipeline_end_time = time.time()
        print("Pipeline took ", pipeline_end_time - pipeline_start_time , " seconds to run!")


if __name__ == "__main__":

    # Get Arguments
    print("Fetching Arguments...")
    job_mode = sys.argv[1]  # local,cluster
    gcs_bucket_name = sys.argv[2]  # remote_gcs bucket name
    config_path = sys.argv[3]  # remote gcs path, for local it will be src/resources/local/config.yaml
    data_source = sys.argv[4] # audio source: joshtalks, brahmakumari
    audio_id = sys.argv[5] # unique identifier for each audio file
    audio_extn = sys.argv[6] # audio file exten: can be .mp4 or .mp3

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
    obj.fit(config_local_path,gcs_bucket_name,data_source,audio_id,audio_extn)
