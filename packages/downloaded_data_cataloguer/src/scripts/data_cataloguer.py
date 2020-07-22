import os
import json
import sys
import yaml

from sqlalchemy import create_engine
from .db_query import upload_file
from .gcs_operations import CloudStorageOperations


class MyDict(dict):
    def __str__(self):
        return json.dumps(self)


def get_file_extension(file_name):
    return file_name.split('.')[-1]


def check_if_meta_data_present(full_source_path, metadata_file_name):
    return obj_gcs_ops.check_blob(bucket_name, full_source_path + '/' + metadata_file_name)


def get_files_path_with_no_prefix(file_path, file_name):
    return file_path + '/' + file_name


class CatalogueDownloadedData:
    def __init__(self):
        self.meta_file_extension = ".csv"

    @staticmethod
    def get_file_name(file_prefix_name, delimiter):
        return file_prefix_name.split(delimiter)[-1]

    def get_metadata_file_name(self, file_name):
        return '.'.join(file_name.split('.')[:-1]) + self.meta_file_extension

    @staticmethod
    def condition_file_name(file_name):
        file_name_cleansed = file_name.translate({ord(i): None for i in "()&'"})
        file_name_split = file_name_cleansed.split('.')
        return '_'.join(file_name_split[:-1]) + '.' + file_name_split[-1]

    def move_and_catalogue_from_download(self, downloaded_source, batch_count, db_conn,
                                         error_landing_path):
        delimiter = "/"
        print("****The source is *****" + downloaded_source)

        all_blobs = obj_gcs_ops.list_blobs_in_a_path(
            bucket_name, source_landing_path + downloaded_source + delimiter)

        try:
            for blob in all_blobs:
                print("*********The file name is ********* " + blob.name)
                file_name = self.get_file_name(blob.name, delimiter)
                file_extension = get_file_extension(file_name)
                expected_file_extension = source_audio_format
                if self.has_mp3_extension(expected_file_extension, file_extension):
                    if batch_count > 0:
                        metadata_file_name = self.get_metadata_file_name(file_name)
                        print("File is {}".format(file_name))
                        source_file_name = get_files_path_with_no_prefix(
                            source_landing_path + downloaded_source, file_name)
                        source_meta_file_name = get_files_path_with_no_prefix(source_landing_path + downloaded_source,
                                                                              metadata_file_name)
                        destination_file_name = get_files_path_with_no_prefix(landing_path + downloaded_source,
                                                                              self.condition_file_name(file_name))
                        destination_meta_file_name = get_files_path_with_no_prefix(landing_path + downloaded_source,
                                                                                   self.condition_file_name(
                                                                                       metadata_file_name))
                        if check_if_meta_data_present(source_landing_path + downloaded_source, metadata_file_name):
                            self.move_and_upload_to_db(db_conn, destination_file_name, destination_meta_file_name,
                                                       metadata_file_name, source_file_name,
                                                       source_meta_file_name)
                        else:
                            self.move_to_error(error_landing_path, file_name, metadata_file_name,
                                               downloaded_source,
                                               source_file_name)
                    else:
                        break
                    batch_count -= 1

        finally:
            print(downloaded_source)

    def move_to_error(self, error_landing_path, file_name, metadata_file_name, source, source_file_name):
        print("Meta file {} is not present,Moving to error....".format(
            metadata_file_name))
        error_destination_file_name = get_files_path_with_no_prefix(
            error_landing_path + source,
            file_name)
        obj_gcs_ops.move_blob(bucket_name, source_file_name, bucket_name, error_destination_file_name)

    def move_and_upload_to_db(self, db_conn, destination_file_name, destination_meta_file_name, metadata_file_name
                            , source_file_name, source_meta_file_name):
        print("Meta file {} is present".format(
            metadata_file_name))
        local_file_name = os.path.join(
            os.getcwd(), "metadatacsv")
        obj_gcs_ops.download_blob(
            bucket_name, source_meta_file_name, local_file_name)
        upload_file(self, local_file_name, db_conn)
        obj_gcs_ops.move_blob(bucket_name, source_file_name, bucket_name, destination_file_name)
        obj_gcs_ops.move_blob(bucket_name, source_meta_file_name, bucket_name, destination_meta_file_name)

    def has_mp3_extension(self, expected_file_extension, file_extension):
        return file_extension in [expected_file_extension, expected_file_extension.swapcase()]


def get_variables(config_file_path):
    config_file = __load_yaml_file(config_file_path)
    configuration = config_file['cataloguer_configuration']
    global bucket_name
    global landing_path
    global source_landing_path
    global error_landing_path
    source_landing_path = configuration['source_landing_path']
    bucket_name = configuration['bucket_name']
    landing_path = configuration['landing_path']
    error_landing_path = configuration['error_landing_path']


def __load_yaml_file(path):
    read_dict = {}
    with open(path, 'r') as file:
        read_dict = yaml.safe_load(file)
    return read_dict


def create_db_engine(config_local_path):
    config_file = __load_yaml_file(config_local_path)
    db_configuration = config_file['db_configuration']
    db_name = db_configuration['db_name']
    db_user = db_configuration['db_user']
    db_pass = db_configuration['db_pass']
    cloud_sql_connection_name = db_configuration['cloud_sql_connection_name']
    db = create_engine(
        f'postgresql://{db_user}:{db_pass}@{cloud_sql_connection_name}/{db_name}')
    return db


if __name__ == "__main__":
    # Get Arguments
    job_mode = sys.argv[1]  # local,cluster
    gcs_bucket_name = sys.argv[2]  # remote_gcs bucket name
    # remote gcs path, for local it will be src/resources/local/config.yaml
    config_path = sys.argv[3]
    downloaded_source = sys.argv[4]
    batch_count = int(sys.argv[5])
    source_audio_format = sys.argv[6]

    current_working_directory = os.getcwd()
    config_local_path = os.path.join(
        current_working_directory, "src/resources/" + job_mode + "/config.yaml")

    if job_mode == "cluster":
        # Download config file from GCS
        print("Downloading config file from cloud storage to local")
        obj_gcs = CloudStorageOperations()
        obj_gcs.download_to_local(bucket_name=gcs_bucket_name,
                                  source_blob_name=config_path,
                                  destination=config_local_path,
                                  is_directory=False)

    get_variables(config_local_path)
    db = create_db_engine(config_local_path)
    obj_gcs_ops = CloudStorageOperations()
    connection = db.connect()

    cataloguer = CatalogueDownloadedData()
    cataloguer.move_and_catalogue_from_download(downloaded_source, batch_count, db, error_landing_path)
