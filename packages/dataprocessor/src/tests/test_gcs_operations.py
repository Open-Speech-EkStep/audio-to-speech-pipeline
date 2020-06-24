import unittest
import datetime
from unittest import mock
import os
from src.scripts.gcs_operations import CloudStorageOperations


class TestGCSOperation(unittest.TestCase):

    # def test_get_audio_id(self):
    #     ob = CloudStorageOperations()
    #     self.assertEqual(ob.get_audio_id(), datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')[:-2]) #Delay in function call fails the test

    def test_make_directories(self):
        ob = CloudStorageOperations()
        ob.make_directories("test")
        self.assertEqual(True, os.path.isdir("test"))
        os.removedirs("test")

    @mock.patch("google.cloud.storage.Client")
    def test_download_to_local_with_file_mode(self, mock_client):
        ob = CloudStorageOperations()
        ob.download_to_local("Bucket", "source_blob_name", "tests", False, exclude_extn=None)
        bucket = mock_client().bucket
        bucket.assert_called_with("Bucket")
        os.removedirs("test")

    @mock.patch("google.cloud.storage.Client.list_blobs")
    @mock.patch("google.cloud.storage.Client")
    def test_download_to_local_with_directory_mode(self, mock_blob, mock_client):
        ob = CloudStorageOperations()
        ob.download_to_local("Bucket", "source_blob_name", "tests", True, exclude_extn=None)
        blob = mock_blob().list_blobs
        blob.assert_called_with("Bucket", prefix='source_blob_name')
        os.removedirs("tests")

    @mock.patch("google.cloud.storage.Client")
    def test_upload_to_gcs_with_directory_mode(self, mock_client):
        ob = CloudStorageOperations()
        ob.upload_to_gcs("Bucket", "src/tests/test_resources/res", "destination_blob_name", True)
        bucket = mock_client().bucket
        bucket.assert_called_with("Bucket")
        destination_blob = mock_client().bucket().blob
        destination_blob.assert_called_with('destination_blob_name/20052020_075124_1.mp4')
        blob_upload = destination_blob().upload_from_filename
        blob_upload.assert_called_with('src/tests/test_resources/res/20052020_075124_1.mp4')

    @mock.patch("google.cloud.storage.Client")
    def test_upload_to_gcs_with_file_mode(self, mock_client):
        ob = CloudStorageOperations()
        ob.upload_to_gcs("Bucket", "src/tests/test_resources/res/20052020_075124_1.mp4", "destination_blob_name", False)
        bucket = mock_client().bucket
        bucket.assert_called_with("Bucket")
        destination_blob = mock_client().bucket().blob
        destination_blob.assert_called_with('destination_blob_name')
        blob_upload = destination_blob().upload_from_filename
        blob_upload.assert_called_with('src/tests/test_resources/res/20052020_075124_1.mp4')


if __name__ == '__main__':
    unittest.main()
