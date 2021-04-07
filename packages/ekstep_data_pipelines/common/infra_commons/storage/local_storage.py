import os
from shutil import copyfile

from ekstep_data_pipelines.common.infra_commons.storage import BaseStorageInterface
from ekstep_data_pipelines.common.infra_commons.storage.exceptions import (
    FileNotFoundException,
)


class LocalStorage(BaseStorageInterface):
    def __init__(self, **kwargs):
        pass

    def list_files(self, source_path: str):
        all_path = os.listdir(source_path)
        paths = []
        for path in all_path:
            if os.path.isdir(f"{source_path}/{path}"):
                path = f"{path}/"
            paths.append(path)
        return path

    def download_folder_to_location(self, source_path: str, destination_path: str):
        os.system(f"cp -r {source_path} {destination_path}")

    def download_to_location(self, source_path: str, destination_path: str):
        os.system(f"cp {source_path} {destination_path}")

    def upload_to_location(self, source_path: str, destination_path: str):
        os.system(f"cp {source_path} {destination_path}")

    def upload_folder_to_location(self, source_path: str, destination_path: str):
        path_to_create = destination_path.split("/")
        path_to_create.pop()
        path_to_create = "/".join(path_to_create)
        if not os.path.isdir(path_to_create):
            os.makedirs(path_to_create)
        os.system(f"cp -r {source_path} {destination_path}")

    def download_file_to_location(self, source_path: str, download_location: str):
        self.copy(source_path, download_location)

    def move(self, source_path: str, destination_path: str) -> bool:
        copied = self.copy(source_path, destination_path)

        if not copied:
            return False

        return self.delete(source_path)

    def copy(self, source_path: str, destination_path: str) -> bool:

        if not self.path_exists(source_path):
            raise FileNotFoundException(f"File at {source_path} not found")

        copyfile(source_path, destination_path)

        return True

    def delete(self, path: str) -> bool:

        if not self.path_exists(path):
            raise FileNotFoundException(f"{path} not found")

        os.remove(path)

        return True

    def path_exists(self, path: str) -> bool:
        return os.path.exists(path)
