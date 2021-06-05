import sys
import multiprocessing
import os

from ekstep_data_pipelines.audio_analysis.constants import (
    CONFIG_NAME,
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
    ULCA_PARAMS = 'ULCA_PARAMS'
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

        self.ulca_config = self.data_processor.config_dict.get(CONFIG_NAME)

        remote_base_path = self.ulca_config.get("source_path")

        source = self.get_source_from_config(**kwargs)
        parameters = self.get_params()

        local_audio_download_path = f"{ULCADataset.DEFAULT_DOWNLOAD_PATH}/{source}/"
        self.ensure_path(local_audio_download_path)

        LOGGER.info(f"Ensured {local_audio_download_path} exists")

        LOGGER.info("Total available cpu count:" + str(multiprocessing.cpu_count()))


    def get_full_path(self, source):
        remote_file_path = self.audio_analysis_config.get(REMOTE_PROCESSED_FILE_PATH)
        remote_download_path = f"{remote_file_path}/{source}"
        return remote_download_path

    def ensure_path(self, path):
        os.makedirs(path, exist_ok=True)

    def get_source_from_config(self, **kwargs):
        source = kwargs.get("source")

        if source is None:
            raise Exception("filter by source is mandatory")

        return source

    def get_params(self):
        return self.audio_analysis_config.get(ULCADataset.ULCA_PARAMS)

    def create_data_json(self, text_dict,
            source,language,
            catalogue_dao):
        LOGGER.info('Creating json')
        utterances = catalogue_dao.get_utterance_details_by_source(source, language)
        LOGGER.info('utterances', type(utterances))
        data = [self.to_data_element(utterance, source, text_dict) for utterance in utterances]
        data = filter(lambda d: d != {}, data)
        return list(data)

    def to_data_element(self, utterance, source, text_dict):
        file_name = utterance[0]
        duration = utterance[1]
        snr = utterance[2]
        main_source_url = utterance[4]
        source_url = utterance[5]
        snr = {
            "methodType": "WadaSnr",
            "methodDetails": {
                "snr": snr
            }
        }
        if (file_name in text_dict ):
            text = text_dict.get(file_name, "")
            return {
                "audioFilename": file_name,
                "text": text,
                "collectionSource": [source, main_source_url, source_url],
                "snr": snr,
                "duration": duration
            }
        else:
            return {}


