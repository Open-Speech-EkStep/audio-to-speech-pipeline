# import signal
import sys
import multiprocessing
import os

from concurrent.futures import ThreadPoolExecutor


from ekstep_data_pipelines.common.utils import get_logger
from ekstep_data_pipelines.common import BaseProcessor
from ekstep_data_pipelines.audio_embedding.create_embeddings import (
    encode_each_batch
)


LOGGER = get_logger("AudioEmbeddingProcessor")

ESTIMATED_CPU_SHARE = 0.1

class AudioEmbedding(BaseProcessor):
    """
    Class to identify speaker for each utterance in a source
    """

    local_txt_path = "./audio_speaker_cluster/file_path/"
    local_audio_path = "./audio_speaker_cluster/audio_files/"
    embed_file_path = "./audio_speaker_cluster/embed_file_path/"

    @staticmethod
    def get_instance(data_processor, **kwargs):
        return AudioEmbedding(data_processor, **kwargs)

    def __init__(self, data_processor, **kwargs):
        self.data_processor = data_processor
        self.audio_analysis_config = None
        

        super().__init__(**kwargs)

    def process(self, **kwargs):
        """
        Function for mapping utterance to speakers
        """
        LOGGER.info("Audio embedding start...")
        # source = self.get_source_from_config(**kwargs)
        input_file_path = self.get_input_file_path_from_config(**kwargs)

        filename = os.path.basename(input_file_path)
        filename_without_ext = filename.split('.')[0]

        npz_file_name = f'{filename_without_ext}.npz'

        self.ensure_path(self.local_txt_path)
        self.ensure_path(self.local_audio_path)
        self.download_files(input_file_path,self.local_txt_path,self.local_audio_path)


        self.ensure_path(self.embed_file_path)

        self.create_embeddings("*.wav",npz_file_name,self.local_audio_path,self.embed_file_path)
        
        self.upload_to_gcp(npz_file_name,input_file_path)


    def download_files(self,input_file_path,local_txt_path,local_audio_path):
        
        LOGGER.info("Total available cpu count:" + str(multiprocessing.cpu_count()))

        LOGGER.info(
                f"Downloading source from {input_file_path}"
            )
        self.fs_interface.download_file_to_location(
            input_file_path, f'{local_txt_path}{os.path.basename(input_file_path)}'
        )

        text_file = open(f'{local_txt_path}{os.path.basename(input_file_path)}', "r")
        paths = text_file.readlines()
        text_file.close()

        worker_pool = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() / ESTIMATED_CPU_SHARE)

        for file_path in paths:
            file_name = os.path.basename(file_path.rstrip("\n"))
            worker_pool.submit(self.fs_interface.download_file_to_location, file_path.rstrip('\n'), f'{local_audio_path}{file_name}')
        worker_pool.shutdown(wait=True)


    def create_embeddings(self,dir_pattern,local_npz_file,local_audio_path,embed_file_path):
        encode_each_batch(local_audio_path, dir_pattern, f'{embed_file_path}{local_npz_file}')

    def upload_to_gcp(self,local_npz_file,input_file_path):
        npz_bucket_destination_path = f'{os.path.dirname(input_file_path)}/{local_npz_file}'

        is_uploaded = self.fs_interface.upload_to_location(
            f'{self.embed_file_path}{local_npz_file}', npz_bucket_destination_path
        )
        if is_uploaded:
            LOGGER.info("npz file uploaded to :" + npz_bucket_destination_path)
        else:
            LOGGER.info("npz file could not be uploaded to :" + npz_bucket_destination_path)


    def ensure_path(self, path):
        os.makedirs(path, exist_ok=True)

    def get_input_file_path_from_config(self, **kwargs):
        input_file_path = kwargs.get("file_path")

        if input_file_path is None:
            raise Exception("filter by source is mandatory")

        return input_file_path

