from abc import ABC, abstractmethod


class BaseStorageInterface(ABC):
    @abstractmethod
    def list_files(self, source_path: str):
        pass

    @abstractmethod
    def download_to_location(self, source_path: str, destination_path: str):
        pass

    @abstractmethod
    def download_folder_to_location(self, source_path: str, destination_path: str):
        pass

    @abstractmethod
    def upload_to_location(self, source_path: str, destination_path: str):
        pass

    @abstractmethod
    def upload_folder_to_location(self, source_path: str, destination_path: str):
        pass

    @abstractmethod
    def move(self, source_path: str, destination: str) -> bool:
        pass

    @abstractmethod
    def copy(self, source_path: str, destination: str) -> bool:
        pass

    @abstractmethod
    def delete(self, path: str) -> bool:
        pass

    @abstractmethod
    def path_exists(self, path: str) -> bool:
        pass


def get_storage_clients(initlization_dict):
    # cyclic. Possible fix - Can move the interface to someother file
    from .google_storage import GoogleStorage
    from .local_storage import LocalStorage

    storage_clients = {}

    storage_clients["local"] = LocalStorage()
    storage_clients["google"] = GoogleStorage()

    return storage_clients
