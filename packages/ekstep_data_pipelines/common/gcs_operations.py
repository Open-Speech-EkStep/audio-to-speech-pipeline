import os
import yaml
import shutil, glob
from os import listdir
from os.path import isfile, join
from google.cloud import storage
import datetime


class CloudStorageOperations():

    @staticmethod
    def get_instance(intialization_dict):
        gcs_instance = CloudStorageOperations(**intialization_dict)
        gcs_instance.setup_peripherals()
        return gcs_instance

    def __init__(self, **kwargs):
        self.config_file_path = kwargs.get('config_file_path')
        self.config_dict = None
        self._bucket = None
        self._client = None

    def setup_peripherals(self):
        # get yaml config
        self.load_configeration()

    def load_configeration(self):
        """
        Load up configeration
        """

        if not self.config_file_path:
            # TODO: ideally raise exception here
            pass

        with open(self.config_file_path, 'r') as file:
            parent_config_dict = yaml.load(file)
            self.config_dict = parent_config_dict.get('config')

    @property
    def client(self):
        if self._client:
            return self._client

        self._client = storage.Client()
        return self._client

    @property
    def bucket(self):
        if self._bucket:
            return self._bucket

        if not self.config_dict:
            self.setup_peripherals()


        self._bucket = self.config_dict.get('common', {}).get('gcs_config', {}).get('master_bucket')
        return self._bucket


    def check_path_exists(self, path):
        bucket = self.client.bucket(self.bucket)
        stats = storage.Blob(bucket=bucket, name=path).exists(self.client)
        return stats

    def copy_all_files(self, src, dest, audio_extn):
        src_files = glob.glob(src + '/*.' + audio_extn)
        print("*******src_files***",src,src_files,audio_extn)
        for file_name in src_files:
            meta_file_name = '/'.join(file_name.split('/')[:-1]) + '/' + file_name.split('/')[-1].split('.')[0] + '.csv'
            full_meta_file_name = os.path.join(src, meta_file_name)
            full_file_name = os.path.join(src, file_name)
            print("*******full_meta_file_name****", full_meta_file_name)
            print("*******full_file_name****", full_file_name)
            if os.path.isfile(full_file_name) and os.path.isfile(full_meta_file_name):
                destination = dest + '/' + self.get_audio_id()
                self.make_directories(destination)
                print("****dest***",destination)
                shutil.copy(full_file_name, destination)
                shutil.copy(full_meta_file_name, destination)

    def get_audio_id(self):
        return datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')[:-2]

    def make_directories(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
            print("Directory {} created successfully".format(path))
        else:
            print("Directory {} already exists".format(path))

    def download_to_local(self, bucket_name, source_blob_name, destination, is_directory, exclude_extn=None):
        """Downloads a blob from the bucket."""
        # Provides options to download a file OR folder
        # Option 1: FILE mode: Download a file - copies a file with same name in destination folder
        # bucket_name = "your-bucket-name"
        # source_blob_name = "storage-object-name" e.g. "data/raw/curation/tobeprocessed/hindi/f10.txt"
        # destination = "local/path/to/folder" e.g. "data/raw/curation/tobeprocessed/hindi/f10.txt"
        # isDirectory = flag to specify whether source is Directory OR File

        # Option 2: DIRECTORY mode: Download all files inside a folder - creates destination local dir if not exists and copies all files from source
        # bucket_name = "your-bucket-name"
        # source_blob_name = "storage-object-name" e.g. "data/raw/curation/tobeprocessed/hindi"
        # destination = "local/path/to/folder" e.g. "data/raw/curation/tobeprocessed/hindi"
        # isDirectory = flag to specify whether source is Directory OR File

        print("Creating storage client object")
        storage_client = storage.Client()
        if (is_directory):
            # Create destination directories if not exists
            print("Running in DIRECTORY mode...")
            print("Creating destination directories if not exists")

            self.make_directories(destination)
            print("Fetching all blobs list from Bucket: {} and Source: {}".format(bucket_name, source_blob_name))

            blobs = list(storage_client.list_blobs(bucket_name, prefix=source_blob_name))
            print("Fetched all blobs list successfully")
            print("Will exclude {} extension file while copying to local destination".format(exclude_extn))

            for blob in blobs:
                if ((not blob.name.endswith("/")) & (
                        not blob.name[blob.name.rfind("/") + 1:len(blob.name)].split(".")[1] == exclude_extn)):
                    print("Downloading blob {}/{} to local directory: {}: ".format(bucket_name, blob.name, destination))
                    blob.download_to_filename(blob.name)
                    print("Blob downloaded successfully: {}".format(blob.name))
        else:
            print("Running in FILE mode...")

            # Get the Destination directory from input
            destination_directory = destination[0:destination.rfind("/")]
            print("Destination directory to be used for file download: {}".format(destination_directory))
            print("Creating destination directories if not exists")
            self.make_directories(destination_directory)

            bucket = storage_client.bucket(bucket_name)
            src_blob = bucket.blob(source_blob_name)

            # Download the file
            print("Downloading file {} to destination: {}".format(source_blob_name, destination_directory))
            src_blob.download_to_filename(destination)
            print("File {}/{} downloaded to destination directory {} successfully".format(bucket_name, source_blob_name,
                                                                                          destination_directory))

    def upload_to_gcs(self, bucket_name, source, destination_blob_name, is_directory):
        """Uploads a blob from the local."""
        # Provides options to upload a file OR folder
        # Option 1: FILE mode: Upload a file - copies a file with same name in destination bucket folder
        # bucket_name = "your-bucket-name"
        # source =  "local/path/to/folder/file" e.g. "data/raw/curation/tobeprocessed/hindi/f10.txt"
        # destination_blob_name = "storage-object-name" e.g. "data/raw/curation/tobeprocessed/hindi/f10.txt"
        # isDirectory = flag to specify whether source is Directory OR File

        # Option 2: DIRECTORY mode: Upload all files inside a folder to cloud storage
        # bucket_name = "your-bucket-name"
        # source =  "local/path/to/folder" e.g. "data/raw/curation/tobeprocessed/hindi"
        # destination_blob_name = "storage-object-name" e.g. "data/raw/curation/tobeprocessed/hindi"
        # isDirectory = flag to specify whether source is Directory OR File

        print("Creating storage client object")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        if (is_directory):
            print("Running in DIRECTORY mode...")
            print("Fetching list of files to be uploaded ")
            files = [f for f in listdir(source) if isfile(join(source, f))]
            for file in files:
                src_file = source + "/" + file
                blob = bucket.blob(destination_blob_name + "/" + file)
                print("Uploading files from source: {} to destination: {}/{} ".format(src_file, bucket_name, blob.name))
                blob.upload_from_filename(src_file)
            print("All files uploaded successfully")
        else:
            print("Running in FILE mode...")
            print("Uploading file from source: {} to destination: {}/{} ".format(source, bucket_name,
                                                                                 destination_blob_name))
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(source)
            print("File uploaded successfully to {}/{}".format(bucket_name, destination_blob_name))

    def list_blobs(self, bucket_name, prefix, delimiter=None):
        """Lists all the blobs in the bucket."""
        # bucket_name = "your-bucket-name"

        storage_client = storage.Client()
        # Note: Client.list_blobs requires at least package version 1.17.0.
        blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

        for blob in blobs:
            print(blob.name)
        if delimiter:
            print("Prefixes:")
            for prefix in blobs.prefixes:
                print(prefix)

    def rename_blob(self, bucket_name, blob_name, new_name):
        """Renames a blob."""
        # bucket_name = "your-bucket-name"
        # blob_name = "your-object-name"
        # new_name = "new-object-name"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        new_blob = bucket.rename_blob(blob, new_name)

        print("Blob {}/{} has been renamed to {}".format(bucket_name, blob.name, new_blob.name))

    def copy_blob(self, blob_name, destination_blob_name,destination_bucket_name=None):
        """Copies a blob from one bucket to another with a new name."""
        # bucket_name = "your-bucket-name"
        # blob_name = "your-object-name"
        # destination_bucket_name = "destination-bucket-name"
        # destination_blob_name = "destination-object-name"
        if not destination_bucket_name:
            destination_bucket_name = self.bucket

        storage_client = storage.Client()

        source_bucket = storage_client.bucket(self.bucket)
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)

        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name
        )

        print(
            "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                blob_copy.name,
                destination_bucket.name,
            )
        )

    def list_blobs_in_a_path(self, file_prefix, delimiter=None):
        """Lists all the blobs in the bucket."""
        # bucket_name = "your-bucket-name"
        print("*****File prefix is ***** " + file_prefix)
        storage_client = storage.Client()

        # Note: Client.list_blobs requires at least package version 1.17.0.
        blobs = storage_client.list_blobs(self.bucket, prefix=file_prefix, delimiter=delimiter)
        return blobs

    def move_blob(self, blob_name, destination_blob_name,destination_bucket_name=None):

        if not destination_bucket_name:
            destination_bucket_name = self.bucket

        source_blob = self.copy_blob_for_move(self.bucket, blob_name, destination_bucket_name, destination_blob_name)
        source_blob.delete()
        print("Blob {} deleted.".format(source_blob))

    @staticmethod
    def copy_blob_for_move(
            bucket_name, blob_name, destination_bucket_name, destination_blob_name
    ):
        """Copies a blob from one bucket to another with a new name."""
        # bucket_name = "your-bucket-name"
        # blob_name = "your-object-name"
        # destination_bucket_name = "destination-bucket-name"
        # destination_blob_name = "destination-object-name"

        storage_client = storage.Client()

        source_bucket = storage_client.bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)

        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name
        )

        print(
            "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                blob_copy.name,
                destination_bucket.name,
            )
        )
        return source_blob
