import os

from audio_analysis import analyse_speaker
from audio_processing.constants import CONFIG_NAME, REMOTE_RAW_FILE
from common.utils import get_logger
from common import BaseProcessor


Logger = get_logger("AudioSpeakerClusteringProcessor")

class AudioAnalysis(BaseProcessor):

    """
    Class to identify speaker for each utterance in a source
    """

    DEFAULT_DOWNLOAD_PATH = '/tmp/audio_speaker_cluster'

    @staticmethod
    def get_instance(data_processor, **kwargs):
        return AudioAnalysis(data_processor, **kwargs)

    def __init__(self, data_processor, **kwargs):
        self.data_processor = data_processor
        self.audio_processor_config = None
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
        Logger.info(f'Downloading source to {local_audio_download_path}')
        self.ensure_path(local_audio_download_path)
        Logger.info(f'Ensured {local_audio_download_path} exists')
        remote_download_path = self.get_full_path(source)
        self.fs_interface.download_folder_to_location(remote_download_path, local_audio_download_path, 5)

        analyse_speaker(embed_file_path, '*/clean/*.wav', local_audio_download_path, source)

    def get_full_path(self, source):
        remote_file_path = self.audio_processor_config.get(REMOTE_RAW_FILE)
        remote_download_path = f'{remote_file_path}/{source}'
        return remote_download_path

    def ensure_path(self, path):
        os.makedirs(path, exist_ok=True)
