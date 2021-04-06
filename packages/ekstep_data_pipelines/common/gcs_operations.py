import datetime
import glob
import multiprocessing
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from os import listdir
from os.path import isfile, join

from google.cloud import storage
from ekstep_data_pipelines.common.utils import get_logger

Logger = get_logger("GCS Operations")


class CloudStorageOperations:
    @staticmethod
    def get_instance(config_dict, **kwargs):
        gcs_instance = CloudStorageOperations(config_dict, **kwargs)
        return gcs_instance

    def __init__(self, config_dict, **kwargs):
        self.config_dict = config_dict
        self._bucket = None
        self._client = None

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

        # if not self.config_dict:
        # self.setup_peripherals()

        self._bucket = (
            self.config_dict.get("common", {})
                .get("gcs_config", {})
                .get("master_bucket")
        )
        return self._bucket

    def check_path_exists(self, path):
        bucket = self.client.bucket(self.bucket)
        stats = storage.Blob(bucket=bucket, name=path).exists(self.client)
        return stats

    def copy_all_files(self, src, dest, audio_extn):
        src_files = glob.glob(src + "/*." + audio_extn)
        print("*******src_files***", src, src_files, audio_extn)
        for file_name in src_files:
            meta_file_name = (
                    "/".join(file_name.split("/")[:-1])
                    + "/"
                    + file_name.split("/")[-1].split(".")[0]
                    + ".csv"
            )
            full_meta_file_name = os.path.join(src, meta_file_name)
            full_file_name = os.path.join(src, file_name)
            print("*******full_meta_file_name****", full_meta_file_name)
            print("*******full_file_name****", full_file_name)
            if os.path.isfile(full_file_name) and os.path.isfile(
                    full_meta_file_name):
                destination = dest + "/" + self.get_audio_id()
                self.make_directories(destination)
                print("****dest***", destination)
                shutil.copy(full_file_name, destination)
                shutil.copy(full_meta_file_name, destination)

    def get_audio_id(self):
        return datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-2]

    def make_directories(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
            print("Directory {} created successfully".format(path))
        else:
            print("Directory {} already exists".format(path))

    def download_to_local(
            self, source_blob_name, destination, is_directory, exclude_extn=None
    ):
        """Downloads a blob from the bucket."""
        # Provides options to download a file OR folder
        # Option 1: FILE mode: Download a file - copies a file with same name in destination folder
        # bucket_name = "your-bucket-name"
        # source_blob_name = "storage-object-name" e.g. "data/raw/curation/"
        # "tobeprocessed/hindi/f10.txt"
        # destination = "local/path/to/folder" e.g. "data/raw/curation/tobeprocessed/hindi/f10.txt"
        # isDirectory = flag to specify whether source is Directory OR File

        # Option 2: DIRECTORY mode: Download all files inside a folder - creates destination
        # local dir if not exists and copies all files from source
        # bucket_name = "your-bucket-name"
        # source_blob_name = "storage-object-name" e.g. "data/raw/curation/tobeprocessed/hindi"
        # destination = "local/path/to/folder" e.g. "data/raw/curation/tobeprocessed/hindi"
        # isDirectory = flag to specify whether source is Directory OR File

        print("Creating storage client object")
        storage_client = storage.Client()
        if is_directory:
            # Create destination directories if not exists
            print("Running in DIRECTORY mode...")
            print("Creating destination directories if not exists")

            self.make_directories(destination)
            print(
                "Fetching all blobs list from Bucket: {} and Source: {}".format(
                    self.bucket, source_blob_name))

            blobs = list(
                storage_client.list_blobs(self.bucket, prefix=source_blob_name)
            )
            print("Fetched all blobs list successfully")
            print(
                "Will exclude {} extension file while copying to local destination".format(
                    exclude_extn
                )
            )

            for blob in blobs:
                if (not blob.name.endswith("/")) & (not blob.name[blob.name.rfind(
                        "/") + 1: len(blob.name)].split(".")[1] == exclude_extn):
                    print(
                        "Downloading blob {}/{} to local directory: {}: ".format(
                            self.bucket, blob.name, destination))
                    blob.download_to_filename(
                        destination + "/" + blob.name.split("/")[-1]
                    )
                    print("Blob downloaded successfully: {}".format(blob.name))
        else:
            print("Running in FILE mode...")

            # Get the Destination directory from input
            destination_directory = destination[0: destination.rfind("/")]
            print(
                "Destination directory to be used for file download: {}".format(
                    destination_directory
                )
            )
            print("Creating destination directories if not exists")
            self.make_directories(destination_directory)

            bucket = storage_client.bucket(self.bucket)
            src_blob = bucket.blob(source_blob_name)

            # Download the file
            print(
                "Downloading file {} to destination: {}".format(
                    source_blob_name, destination_directory
                )
            )
            src_blob.download_to_filename(destination)
            print(
                "File {}/{} downloaded to destination directory {} successfully".format(
                    self.bucket, source_blob_name, destination_directory
                )
            )

    def upload_to_gcs(
            self, local_source_path, destination_blob_name, upload_directory=True
    ):
        """
        Uploads a blob from the local.

        :param string local_source_path: Local path to the file/directory being uploaded.
                                         Must include the file name incase of file upload

        :param string destination_blob_name: Remote path where the file/directory needs
        to be uploaded to

        :param bool upload_directy: Flag for specifying if the function is being used to
                                    upload a file or a directory. Pass false incase of file

        """

        bucket = self.client.bucket(self.bucket)

        if not upload_directory:
            Logger.info(
                "Uploading file from source: %s to destination: "
                "%f/%f", local_source_path, self.bucket, destination_blob_name
            )
            blob = bucket.blob(destination_blob_name)
            try:
                blob.upload_from_filename(local_source_path)
            # W0703: Catching too general exception Exception (broad-except)
            except Exception as exception:
                Logger.info(
                    "Single file Upload failed with error %s", exception.__str__())
                return False

            Logger.info(
                "Single File uploaded successfully to %f/%f", self.bucket, destination_blob_name
            )
            return True

        files = [
            f for f in listdir(local_source_path) if isfile(
                join(
                    local_source_path,
                    f))]
        Logger.info("All the files in directory %s", files)
        # TODO: move to constant and pass concurrency as args
        estimated_cpu_share = 0.05
        concurrency = multiprocessing.cpu_count() / estimated_cpu_share
        executor = ThreadPoolExecutor(max_workers=concurrency)

        futures = []

        for file in files:
            src_file = local_source_path + "/" + file
            blob = bucket.blob(destination_blob_name + "/" + file)
            Logger.info(
                "Uploading files from source: {} to destination: {}/{} ".format(
                    src_file, self.bucket, blob.name))
            futures.append(
                executor.submit(
                    blob.upload_from_filename,
                    src_file))

        executor.shutdown(wait=True)

        Logger.info("Checking the result of all upload values")

        for upload_future in futures:
            try:
                upload_future.result()
            except Exception as exception:
                Logger.error(
                    "Uploading directory %s failed with error %s",
                    local_source_path, exception.__str__()
                )
                return False

        Logger.info(
            "All the files in directory %s uploaded successfully", local_source_path
        )
        return True

    def list_blobs(self, bucket_name, prefix, delimiter=None):
        """Lists all the blobs in the bucket."""
        # bucket_name = "your-bucket-name"

        storage_client = storage.Client()
        # Note: Client.list_blobs requires at least package version 1.17.0.
        blobs = storage_client.list_blobs(
            bucket_name, prefix=prefix, delimiter=delimiter
        )

        for blob in blobs:
            print(blob.name)
        if delimiter:
            print("Prefixes:")
            for prefix_ in blobs.prefixes:
                print(prefix_)

    def rename_blob(self, bucket_name, blob_name, new_name):
        """Renames a blob."""
        # bucket_name = "your-bucket-name"
        # blob_name = "your-object-name"
        # new_name = "new-object-name"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        new_blob = bucket.rename_blob(blob, new_name)

        print(
            "Blob {}/{} has been renamed to {}".format(
                bucket_name, blob.name, new_blob.name
            )
        )

    def copy_blob(
            self,
            blob_name,
            destination_blob_name,
            destination_bucket_name=None):
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
        blobs = storage_client.list_blobs(
            self.bucket, prefix=file_prefix, delimiter=delimiter
        )
        return blobs

    def move_blob(
            self,
            blob_name,
            destination_blob_name,
            destination_bucket_name=None):

        if not destination_bucket_name:
            destination_bucket_name = self.bucket

        source_blob = self.copy_blob_for_move(
            self.bucket,
            blob_name,
            destination_bucket_name,
            destination_blob_name)
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

    def delete_object(self, dir_path):

        bucket = self.client.bucket(self.bucket)
        # blob = bucket.blob(f'{dir_path}')

        all_file = self.list_blobs_in_a_path(dir_path)

        for file in all_file:
            blob = bucket.blob(file.name)
            blob.delete()
            print("Blob {} deleted.".format(file.name))

    def download_blob(self, source_blob_name, destination_file_name):
        # """Downloads a blob from the bucket."""
        # bucket_name = "your-bucket-name"
        # source_blob_name = "storage-object-name"
        # destination_file_name = "local/path/to/file"

        storage_client = storage.Client()

        bucket = storage_client.bucket(self.bucket)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

        print(
            "Blob {} from Bucket {} downloaded to {}.".format(
                source_blob_name, self.bucket, destination_file_name
            )
        )
