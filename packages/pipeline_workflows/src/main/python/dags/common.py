import re
import sys
import pandas as pd
import os
from os import listdir
from os.path import isfile, join
import gswrap
# os.system("export GOOGLE_APPLICATION_CREDENTIALS=~/ekstepspeechrecognition-532504f2d2c1-composer.json")
# [START storage_upload_file]
from google.cloud import storage


def upload_files(bucket_name, srcFolderPath, bucketFolder):
    """Upload files to GCP bucket."""
    files = [f for f in listdir(srcFolderPath) if isfile(join(srcFolderPath, f))]
    for file in files:
        srcFile = srcFolderPath + file
        print("file path: ", srcFile)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(bucketFolder + file)
        print("blob path: ", blob)
        # upload_blob(bucket_name,srcFile,destBlob)
        blob.upload_from_filename(srcFile)
    return f'Uploaded {files} to "{bucket_name}" bucket.'


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}/{}.".format(
            source_file_name, bucket_name, destination_blob_name
        )
    )


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Blob {} from Bucket {} downloaded to {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )


def list_blobs(bucket_name, file_prefix=None):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=file_prefix)

    for blob in blobs:
        print(blob.name)


def rename_blob(bucket_name, blob_name, new_name):
    """Renames a blob."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    # new_name = "new-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    new_blob = bucket.rename_blob(blob, new_name)

    print("Blob {}/{} has been renamed to {}".format(bucket_name, blob.name, new_blob.name))


def move_blob(
        bucket_name, blob_name, destination_bucket_name, destination_blob_name
):
    source_blob = copy_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name)
    source_blob.delete()
    print("Blob {} deleted.".format(source_blob))


def copy_blob(
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


def list_blobs_in_a_path(bucket_name, file_prefix, delimiter=None):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"
    print("*****File prefix is ***** " + file_prefix)
    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=file_prefix, delimiter=delimiter)
    return blobs


def check_blob(bucket_name, file_prefix):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    stats = storage.Blob(bucket=bucket, name=file_prefix).exists(storage_client)
    return stats


def read_blob(bucket_name, file_prefix):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(file_prefix)
    return blob.download_as_string().decode("utf-8").strip()


def move_directory(bucket_name, source_path, destination_path):
    client = gswrap.Client()
    client.cp(
        src="gs://" + bucket_name + '/' + source_path,
        dst="gs://" + bucket_name + '/' + destination_path,
        recursive=True)
    client.rm("gs://" + bucket_name + '/' + source_path, recursive=True)
    return "Source " + source_path + " has been move to " + destination_path


if __name__ == "__main__":
    client = gswrap.Client()
    client.cp(
        src="gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/tobeprocessed/hindi/audio/joshtalks2/20200526134108684454",
        dst="gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/processed/hindi/audio/joshtalks2/20200526134108684454",
        recursive=True)

    # blob_string=read_blob("ekstepspeechrecognition-dev","data/audiotospeech/raw/tobeprocessed/hindi/audio/joshtalks/audio_id/20200520225542180482/joshtalks_audio_id.txt")
    # print(blob_string.splitlines())

    # copy_blob("ekstepspeechrecognition-dev","data/audiotospeech/raw/tobeprocessed/hindi/audio/joshtalks2/20200526131714455493/*.mp4","ekstepspeechrecognition-dev",
    #                                           "data/audiotospeech/raw/processed/hindi/audio/joshtalks2/20200526131714455493/")
    #

    # rename_blob(
    #     bucket_name="sample-project-274716-bucket",
    #     blob_name="data2/testfile1.txt",
    #     new_name="data2/testfile2.txt"
    # )

    # copy_blob(
    #     bucket_name="sample-project-274716-bucket",
    #     blob_name="data2/testfile2.txt",
    #     destination_bucket_name="sample-project-274716-bucket",
    #     destination_blob_name="data/testfile2.txt",
    # )
    # check_blob("ekstepspeechrecognition-dev","data/audiotospeech/raw/landing/hindi/audio/joshtalks/0.txt")
    # list_blobs_in_a_path("ekstepspeechrecognition-dev","data/audiotospeech/raw/landing/hindi/audio/joshtalks/",delimiter="/")
    # print("only files")
    # list_blobs("ekstepspeechrecognition-dev")

    # upload_blob(
    #     bucket_name="ekstepspeechrecognition-dev",   #"sample-project-274716-bucket",
    #     source_file_name="/Users/gauravgupta/Documents/Gaurav/Projects/python-dev/data/data2/f20.txt",
    #     destination_blob_name="test/f20.txt"
    # )

    # upload_files(
    #     bucket_name="ekstepspeechrecognition-dev",
    #     srcFolderPath="/Users/gauravgupta/Documents/Gaurav/Projects/python-dev/data/data2/",
    #     bucketFolder="test/"
    # )

    # download_blob(
    #     bucket_name="sample-project-274716-bucket",
    #     source_blob_name="data2/placeholder.txt",
    #     destination_file_name="/Users/gauravgupta/Documents/Gaurav/Projects/placeholder.txt"
    # )

    # from fsplit.filesplit import FileSplit
    #
    # fs = FileSplit(file='/Users/gauravgupta/Documents/Gaurav/Projects/python-dev/data/IITB_10000.txt', splitsize=30, output_dir='/Users/gauravgupta/Documents/Gaurav/Projects/python-dev/data/')

    # df = pd.read_csv('/Users/gauravgupta/Documents/Gaurav/Projects/python-dev/data/IITB_20.txt',nrows=10,header=None,delimiter="\n")
    # df.to_csv("/Users/gauravgupta/Documents/Gaurav/Projects/python-dev/data/test.txt")
    # chunksize = 10
    # TextFileReader
    # for chunk in pd.read_csv("/Users/gauravgupta/Documents/Gaurav/Projects/python-dev/data/IITB_20.txt",delimiter="\n", chunksize=chunksize):
    #     chunk.to_csv("/Users/gauravgupta/Documents/Gaurav/Projects/python-dev/data/test.txt")
