import os
from shutil import copyfile
from ekstep_data_pipelines.common.infra_commons.storage import BaseStorageInterface
from ekstep_data_pipelines.common.infra_commons.storage.exceptions import FileNotFoundException, PathDoesNotExist

class LocalStorage(BaseStorageInterface):

    def __init__(self, **kwargs):
        pass

    def list_files(self, source_path:str):
        pass

    def download_folder_to_location(self, source_path:str, destination_path:str):
        pass

    def download_to_location(self, source_path:str, destination_path:str):
        pass

    def upload_to_location(self, source_path:str, destination_path:str):
        pass


    def upload_folder_to_location(self, source_path:str, destination_path:str):
        pass


    def download_file_to_location(self, source_path: str, download_location:str):
        self.copy(source_path, download_location)


    def move(self, source_path:str, destination_path: str) -> bool:
        copied = self.copy(source_path, destination_path)

        if not copied:
            return False

        return self.delete(source_path)


    def copy(self, source_path:str, destination_path:str) ->bool:

        if not self.path_exists(source_path):
            raise FileNotFoundException(f'File at {source_path} not found')

        copyfile(source_path, destination_path)

        return True

    def delete(self, path:str) -> bool:

        if not self.path_exists(path):
            raise FileNotFoundException(f'{path} not found')

        os.remove(path)

        return True

    def path_exists(self, path: str) -> bool:
        return os.path.exists(path)