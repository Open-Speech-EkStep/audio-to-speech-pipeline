import sys
import multiprocessing
import os, fnmatch
import json
import subprocess

from ekstep_data_pipelines.audio_analysis.constants import (
    REMOTE_PROCESSED_FILE_PATH,
)
from ekstep_data_pipelines.common.utils import get_logger
from ekstep_data_pipelines.common import BaseProcessor, CatalogueDao


ESTIMATED_CPU_SHARE = 0.1

LOGGER = get_logger("ULCADataset")


class ULCADataset(BaseProcessor):
    """
    Class to identify speaker for each utterance in a source
    """

    DEFAULT_DOWNLOAD_PATH = "./ulca"
    ULCA_CONFIG = "ulca_config"
    SOURCE = "source"
    ULCA_PARAMS = "params"
    LANGUAGE = "language"
    SOURCE_PATH = "source_path"
    PUBLISH_PATH = "publish_path"

    @staticmethod
    def get_instance(data_processor, **kwargs):
        return ULCADataset(data_processor, **kwargs)

    def __init__(self, data_processor, **kwargs):
        self.data_processor = data_processor
        self.ulca_config = None
        self.catalogue_dao = CatalogueDao(self.data_processor)
        super().__init__(**kwargs)

    def handle_termination_gracefully(self, signum, frame):
        LOGGER.info(
            f"SIGINT/SIGTERM invoked with the following information {signum}/{frame}"
        )
        sys.exit(1)

    def process(self, **kwargs):
        """
        Function for mapping utterance to speakers
        """
        LOGGER.info("Total available cpu count:" + str(multiprocessing.cpu_count()))

        source, ulca_config, language, source_path, publish_path, params = self.get_config(**kwargs)

        local_audio_download_path = f"{ULCADataset.DEFAULT_DOWNLOAD_PATH}/{source}/"
        self.ensure_path(local_audio_download_path)

        LOGGER.info(f"Ensured {local_audio_download_path} exists")

        LOGGER.info(f"Downloading source to:{local_audio_download_path}")

        max_workers = multiprocessing.cpu_count() / ESTIMATED_CPU_SHARE
        self.fs_interface.download_folder_to_location(
            source_path, local_audio_download_path, max_workers=max_workers
        )

        text_dict = self.read_transcriptions(local_audio_download_path)
        data = self.create_data_json(text_dict, source, language, self.catalogue_dao)
        self.write_json(local_audio_download_path, "data.json", data)
        self.write_json(local_audio_download_path, "params.json", params)

        self.make_tarfile(f"{source}.tar.gz", local_audio_download_path)

        self.publish_artifact(f"{source}.tar.gz", publish_path)

    def write_json(self, local_audio_download_path, filename, data):
        data_json = json.dumps(data, indent=4)
        with open(f"{local_audio_download_path}/{filename}", "w") as f:
            f.write(data_json)

    def get_full_path(self, source):
        remote_file_path = self.audio_analysis_config.get(REMOTE_PROCESSED_FILE_PATH)
        remote_download_path = f"{remote_file_path}/{source}"
        return remote_download_path

    def ensure_path(self, path):
        os.makedirs(path, exist_ok=True)

    def get_config(self, **kwargs):
        source = kwargs.get(ULCADataset.SOURCE)
        ulca_config = json.loads(kwargs.get(ULCADataset.ULCA_CONFIG))
        LOGGER.info('ulca_config:', str(ulca_config))
        language = ulca_config.get(ULCADataset.LANGUAGE)
        source_path = ulca_config.get(ULCADataset.SOURCE_PATH)
        publish_path = ulca_config.get(ULCADataset.PUBLISH_PATH)
        params = ulca_config.get(ULCADataset.ULCA_PARAMS)

        if source is None:
            raise Exception("source is mandatory")

        if ulca_config is None:
            raise Exception("ulca_config is mandatory")

        if language is None:
            raise Exception("language is mandatory")

        if source_path is None:
            raise Exception("source_path is mandatory")

        if publish_path is None:
            raise Exception("publish_path is mandatory")

        if params is None:
            raise Exception("params is mandatory")

        return source, ulca_config, language, source_path, publish_path, params

    def get_params(self):
        return self.ulca_config.get(ULCADataset.ULCA_PARAMS)

    def create_data_json(self, text_dict, source, language, catalogue_dao):
        LOGGER.info(f"Creating json for source:{source}, language={language}")
        utterances = catalogue_dao.get_utterance_details_by_source(source, language)
        LOGGER.info("utterances", type(utterances))
        data = [
            self.to_data_element(utterance, source, text_dict)
            for utterance in utterances
        ]
        data = filter(lambda d: d != {}, data)
        return list(data)

    def to_data_element(self, utterance, source, text_dict):
        file_name = utterance[0]
        duration = utterance[1]
        snr = utterance[2]
        speaker = utterance[3]
        main_source_url = utterance[4]
        source_url = utterance[5]
        gender = utterance[6]
        snr = {"methodType": "WadaSnr", "methodDetails": {"snr": snr}}
        file_name_key = file_name.split(".")[0]
        gender_map  = {
            "m": "male",
            "f": "female"
        }
        if file_name_key in text_dict:
            text = text_dict.get(file_name_key, "")
            return {
                "audioFilename": file_name,
                "text": text,
                "collectionSource": [source, main_source_url, source_url],
                "snr": snr,
                "duration": duration,
                "speaker": speaker,
                "gender": gender_map.get(gender, "non-specified")
            }
        else:
            return {}

    def read_transcriptions(self, local_source_path):
        listOfFiles = os.listdir(local_source_path)
        pattern = "*.txt"
        text_dict = {}
        print("listOfFiles", listOfFiles)
        for entry in listOfFiles:
            if fnmatch.fnmatch(entry, pattern):
                print(entry)
                with open(f"{local_source_path}/{entry}", "r") as reader:
                    transcription = reader.read()
                    file_name = entry.split(".")[0]
                    text_dict[file_name] = transcription
        return text_dict

    def make_tarfile(self, output_filename, source_dir):
        subprocess.call(["tar", "-czvf", output_filename, source_dir])

    def publish_artifact(self, tar_file, publish_path):
        self.fs_interface.upload_to_location(tar_file, publish_path)
