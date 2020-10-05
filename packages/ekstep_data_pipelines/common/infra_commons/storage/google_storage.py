import os
from os import listdir
from os.path import isfile, join
from google.cloud import storage
from . import BaseStorageInterface
from concurrent.futures import ThreadPoolExecutor
from .exceptions import FileNotFoundException, PathDoesNotExist

class GoogleStorage(BaseStorageInterface):

    def __init__(self, **kwargs):
        self._client = None

    def get_bucket_from_path(self, path) -> str:
        if not path:
            return None

        splitted_path = list(filter(None, path.split('/')))

        if len(splitted_path) < 1:
            return None

        return splitted_path[0]

    def get_path_without_bucket(self, path_with_bucket: str) -> str:
        if not path_with_bucket:
            return None

        splitted_path = list(filter(None, path_with_bucket.split('/')))

        if len(splitted_path) < 1:
            return None

        return '/'.join(splitted_path[1:])


    @property
    def client(self):
        if self._client:
            return self._client

        self._client = storage.Client()
        return self._client

    def list_files(self, source_path:str, include_folders=True):
        bucket_name = self.get_bucket_from_path(source_path)
        actual_path = self.get_path_without_bucket(source_path)

        if actual_path and actual_path[-1] != '/':
            actual_path = actual_path + '/'

        blob_name_set = set()
        is_folder = False
        for blob in self.client.list_blobs(bucket_name, prefix=actual_path):
            content = blob.name.replace(actual_path, '')

            if not content:
                continue

            if '/' in content[:-1]:
                if include_folders:
                    content = content.split('/')[0] + '/'
                    is_folder = True
                else:
                    continue

            if not is_folder:
                content = content.replace('/', '')

            if content.replace('/', '') in blob_name_set:
                blob_name_set.remove(content.replace('/', ''))

            blob_name_set.add(content)

        return list(blob_name_set)

    def download_to_location(self, source_path:str, destination_path:str):
        return self.download_file_to_location(source_path, destination_path)

    def download_folder_to_location(self, source_path:str, destination_path:str):
        source_files = self.list_files(source_path)

        curr_executor = ThreadPoolExecutor(max_workers=5)

        for remote_file in source_files:
            curr_executor.submit(self.download_to_location, f'{source_path}/{remote_file}', f'{destination_path}/{remote_file}')

        curr_executor.shutdown(wait=True)


    def upload_to_location(self, source_path:str, destination_path:str):
        bucket = self.client.bucket(self.get_bucket_from_path(destination_path))
        file_path = self.get_path_without_bucket(destination_path)
        blob = bucket.blob(file_path)

        try:
            blob.upload_from_filename(source_path)
        except Exception as e:
            return False

        return True

    def upload_folder_to_location(self, source_path:str, destination_path:str):
        files_for_upload = [f for f in listdir(source_path) if isfile(join(source_path, f))]

        curr_executor = ThreadPoolExecutor(max_workers=5)
        
        for upload_file in files_for_upload:

            curr_executor.submit(self.upload_to_location,f'{source_path}/{upload_file}', f'{destination_path}/{upload_file}')

        curr_executor.shutdown(wait=True)


    def download_file_to_location(self, source_path: str, download_location:str):
        bucket = self.client.bucket(self.get_bucket_from_path(source_path))
        file_path = self.get_path_without_bucket(source_path)
        source_blob = bucket.blob(file_path)
        source_blob.download_to_filename(download_location)

    def move(self, source_path:str, destination_path: str) -> bool:
        copied = self.copy(source_path, destination_path)

        if not copied:
            return False

        return self.delete(source_path)


    def copy(self, source_path:str, destination_path:str) ->bool:

        if not self.path_exists(source_path):
            raise FileNotFoundException(f'File at {source_path} not found')

        source_bucket = self.client.bucket(self.get_bucket_from_path(source_path))
        source_actual_path = self.get_path_without_bucket(source_path)
        source_blob = source_bucket.bucket(source_actual_path)

        destination_bucket = self.client.bucket(self.get_bucket_from_path(destination_path))
        destination_actual_path = self.get_path_without_bucket(destination_path)

        source_bucket.copy_blob(source_blob, destination_bucket, destination_actual_path)
        return True

    def delete(self, path:str) -> bool:

        if not self.path_exists(path):
            raise FileNotFoundException(f'{path} not found')

        bucket = self.client.bucket(self.get_bucket_from_path(path))
        actual_path = self.get_path_without_bucket(path)

        curr_blob = bucket.blob(actual_path)
        curr_blob.delete()

        return True

    def path_exists(self, path: str) -> bool:
        bucket = self.client.bucket(self.get_bucket_from_path(path))
        actual_path = self.get_path_without_bucket(path)
        return storage.Blob(bucket=bucket, name=actual_path).exists(self.client)