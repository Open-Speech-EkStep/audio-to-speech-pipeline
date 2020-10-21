import multiprocessing
import os

from audio_analysis.analyse_speaker import analyse_speakers
from audio_processing.constants import CONFIG_NAME, REMOTE_PROCESSED_FILE_PATH
from common.utils import get_logger
from common import BaseProcessor, CatalogueDao

Logger = get_logger("AudioSpeakerClusteringProcessor")
ESTIMATED_CPU_SHARE = 0.2
class AudioAnalysis(BaseProcessor):

    """
    Class to identify speaker for each utterance in a source
    """

    DEFAULT_DOWNLOAD_PATH = './audio_speaker_cluster'

    @staticmethod
    def get_instance(data_processor, **kwargs):
        return AudioAnalysis(data_processor, **kwargs)

    def __init__(self, data_processor, **kwargs):
        self.data_processor = data_processor
        self.audio_processor_config = None
        self.catalogue_dao = CatalogueDao(self.data_processor)
        super().__init__(**kwargs)


    def process(self, **kwargs):
        """
        Function for mapping utterance to speakers
        """
        self.audio_processor_config = self.data_processor.config_dict.get(
            CONFIG_NAME)

        source = kwargs.get('source')
        embed_file_path = f'{AudioAnalysis.DEFAULT_DOWNLOAD_PATH}/{source}_embed_file.npz'
        local_audio_download_path = f'{AudioAnalysis.DEFAULT_DOWNLOAD_PATH}/{source}/'
        self.ensure_path(local_audio_download_path)
        Logger.info(f'Ensured {local_audio_download_path} exists')
        remote_download_path = self.get_full_path(source)
        Logger.info(f'Downloading source to {local_audio_download_path} from {remote_download_path}')
        Logger.info("Total available cpu count:" + str(multiprocessing.cpu_count()))
        self.fs_interface.download_folder_to_location(remote_download_path, local_audio_download_path, multiprocessing.cpu_count() / ESTIMATED_CPU_SHARE)
        analyse_speakers(embed_file_path, '*.wav', local_audio_download_path, source, self.catalogue_dao)

    def get_full_path(self, source):
        remote_file_path = self.audio_processor_config.get(REMOTE_PROCESSED_FILE_PATH)
        remote_download_path = f'{remote_file_path}/{source}'
        return remote_download_path

    def ensure_path(self, path):
        os.makedirs(path, exist_ok=True)
