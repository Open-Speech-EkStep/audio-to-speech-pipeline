import glob
import os

from ekstep_data_pipelines.audio_processing.constants import (
    CONFIG_NAME,
    REMOTE_RAW_FILE,
    CHUNKING_CONFIG,
    SNR_CONFIG,
    REMOTE_PROCESSED_FILE_PATH,
    MASTER_META_DATA_FILE_PATH,
    SNR_DONE_FOLDER_PATH,
    DUPLICATE_AUDIO_FOLDER_PATH,
)
from ekstep_data_pipelines.audio_processing.generate_hash import (
    get_hash_code_of_audio_file,
)
from ekstep_data_pipelines.common import BaseProcessor
from ekstep_data_pipelines.common.utils import get_logger

Logger = get_logger("Audio Processor")


class AudioProcessor(BaseProcessor):

    """
    Class for breaking a downloaded file into smaller chunks of
    audio files as well as filtering out files with more than an acceptable level
    of Sound to Noise Ratio(or SNR)
    """

    DEFAULT_DOWNLOAD_PATH = "/tmp/audio_processing_raw"

    @staticmethod
    def get_instance(
        data_processor, gcs_instance, audio_commons, catalogue_dao, **kwargs
    ):
        return AudioProcessor(
            data_processor,
            gcs_instance,
            audio_commons,
            catalogue_dao,
            **kwargs)

    def __init__(
            self,
            data_processor,
            gcs_instance,
            audio_commons,
            catalogue_dao,
            **kwargs):
        self.data_processor = data_processor
        self.gcs_instance = gcs_instance
        self.catalogue_dao = catalogue_dao
        self.snr_processor = audio_commons.get("snr_util")
        self.chunking_processor = audio_commons.get("chunking_conversion")
        self.audio_processor_config = None

        super().__init__(**kwargs)

    def process(self, **kwargs):

        Logger.info("SNR UTILITY")
        """
        Function for breaking an audio file into smaller chunks and then
        accepting/rejecting them basis the SNR ratio.
        """
        self.audio_processor_config = self.data_processor.config_dict.get(
            CONFIG_NAME)

        file_name_list = kwargs.get("file_name_list", [])
        source = kwargs.get("source")
        extension = kwargs.get("extension")
        process_master_csv = kwargs.get("process_master_csv", "false")

        Logger.info(f"Processing audio ids {file_name_list}")
        for file_name in file_name_list:
            audio_id = self.catalogue_dao.get_unique_id()

            Logger.info(f"Processing file {file_name} and audio_id {audio_id}")
            self.process_audio_id(audio_id, source, extension, file_name)

        if not process_master_csv:
            return

        master_metadata_file_path = f"{self.audio_processor_config.get(MASTER_META_DATA_FILE_PATH)}/{source}/{source}_master.csv"

        if self.fs_interface.path_exists(master_metadata_file_path):
            self.upload_and_move(master_metadata_file_path, source)

    def process_audio_id(self, audio_id, source, extension, file_name):
        local_audio_download_path = (
            f"{AudioProcessor.DEFAULT_DOWNLOAD_PATH}/{source}/{audio_id}"
        )

        meta_data_file = file_name.replace(f".{extension}", ".csv")

        Logger.info(
            f"Downloading file for audio_id/{audio_id} to {local_audio_download_path}"
        )

        self.ensure_path(local_audio_download_path)
        Logger.info(f"Ensured {local_audio_download_path} exists")

        remote_download_path, remote_download_path_of_metadata = self.get_full_path(
            source, file_name, meta_data_file)

        Logger.info(
            f"Downloading audio file and metadat file from {remote_download_path},{remote_download_path_of_metadata} to {local_audio_download_path}"
        )

        self.fs_interface.download_file_to_location(
            remote_download_path, f"{local_audio_download_path}/{file_name}"
        )
        self.fs_interface.download_file_to_location(
            remote_download_path_of_metadata,
            f"{local_audio_download_path}/{meta_data_file}",
        )

        hash_code = get_hash_code_of_audio_file(
            f"{local_audio_download_path}/{file_name}"
        )

        is_file_exist = self.catalogue_dao.check_file_exist_in_db(
            file_name, hash_code)

        if is_file_exist:

            Logger.info(
                "file is already exist in db moving to duplicate folder")
            base_path_for_duplicate_audio = f"{self.audio_processor_config.get(DUPLICATE_AUDIO_FOLDER_PATH)}/{source}"

            self.move_file_to_done_folder(
                remote_download_path,
                remote_download_path_of_metadata,
                base_path_for_duplicate_audio,
                file_name,
                meta_data_file,
            )
            return

        meta_data_file_path = self._get_csv_in_path(local_audio_download_path)

        Logger.info(f"Conerting the file with audio_id {audio_id} to wav")
        local_converted_wav_file_path = self._convert_to_wav(
            local_audio_download_path, extension
        )

        if not local_audio_download_path:
            return

        Logger.info(
            f"Breaking {audio_id} at {local_converted_wav_file_path} file into chunks"
        )
        chunk_output_path = self._break_files_into_chunks(
            audio_id, local_audio_download_path, local_converted_wav_file_path
        )

        Logger.info(
            f"Processing SNR ratios for the all the chunks for audio_id {audio_id}"
        )
        self._process_snr(
            chunk_output_path,
            meta_data_file_path,
            local_audio_download_path,
            audio_id,
            hash_code,
        )

        processed_remote_file_path = self.audio_processor_config.get(
            REMOTE_PROCESSED_FILE_PATH
        )
        upload_path = f"{processed_remote_file_path}/{source}/{audio_id}"

        clean_file_upload = self.fs_interface.upload_folder_to_location(
            f"{local_audio_download_path}/clean", f"{upload_path}/clean"
        )

        rejected_file_upload = self.fs_interface.upload_folder_to_location(
            f"{local_audio_download_path}/rejected", f"{upload_path}/rejected"
        )

        if not clean_file_upload and not rejected_file_upload:
            Logger.error(
                f"Uploading chunked/snr cleaned files failed for {audio_id} not processing further."
            )

        self.catalogue_dao.upload_file(meta_data_file_path)

        snr_done_base_path = (
            f"{self.audio_processor_config.get(SNR_DONE_FOLDER_PATH)}/{source}"
        )

        self.move_file_to_done_folder(
            remote_download_path,
            remote_download_path_of_metadata,
            snr_done_base_path,
            file_name,
            meta_data_file,
        )

    def move_file_to_done_folder(
        self,
        audio_file_path,
        meta_data_file_path,
        base_path_with_source,
        file_name,
        meta_data_file,
    ):

        snr_done_path_audio_file_path = f"{base_path_with_source}/{file_name}"
        snr_done_path_metadata_file_path = f"{base_path_with_source}/{meta_data_file}"

        Logger.info(
            f"moving {audio_file_path},{meta_data_file_path} to snr done path {base_path_with_source}"
        )

        self.fs_interface.move(audio_file_path, snr_done_path_audio_file_path)
        self.fs_interface.move(
            meta_data_file_path,
            snr_done_path_metadata_file_path)

    def get_full_path(self, source, file_name, meta_data_file):
        remote_file_path = self.audio_processor_config.get(REMOTE_RAW_FILE)
        remote_download_path = f"{remote_file_path}/{source}/{file_name}"
        remote_download_path_of_metadata = (
            f"{remote_file_path}/{source}/{meta_data_file}"
        )

        return remote_download_path, remote_download_path_of_metadata

    def upload_and_move(self, master_metadata_file_path, source):

        meta_data_done_path = (
            f"{self.audio_processor_config.get(SNR_DONE_FOLDER_PATH)}/{source}"
        )

        local_metadata_downloaded_path = "/tmp/master_csv"

        self.fs_interface.download_file_to_location(
            master_metadata_file_path, local_metadata_downloaded_path
        )

        self.catalogue_dao.upload_file_to_downloaded_source(
            local_metadata_downloaded_path
        )

        self.fs_interface.move(master_metadata_file_path, meta_data_done_path)

    def ensure_path(self, path):
        # TODO: make path empty before creating it again
        os.makedirs(path, exist_ok=True)

    def _convert_to_wav(self, source_file_directory, extension):
        Logger.info(
            f"Converting the contents of the local path {source_file_directory} to wav"
        )

        local_output_directory = f"{source_file_directory}/wav"

        self.ensure_path(local_output_directory)
        Logger.info(
            f"Output initialized to local path {local_output_directory}")

        output_file_path, converted = self.chunking_processor.convert_to_wav(
            source_file_directory, output_dir=local_output_directory, ext=extension)

        if not converted:
            return None

        return output_file_path

    def _break_files_into_chunks(
            self,
            audio_id,
            local_download_path,
            wav_file_path):

        Logger.info(f"Chunking audio file at {wav_file_path}")
        local_chunk_output_path = f"{local_download_path}/chunks"
        local_vad_output_path = f"{local_download_path}/vad"

        aggressivness_dict = self.audio_processor_config.get(
            CHUNKING_CONFIG, {"aggressiveness": 2}
        )

        if not isinstance(aggressivness_dict.get("aggressiveness"), int):
            raise Exception(
                f"Aggressiveness must be an int, not {aggressivness_dict}")

        self.ensure_path(local_chunk_output_path)
        Logger.info(f"Ensuring path {local_chunk_output_path}")
        file_name = wav_file_path.split("/")[-1]

        aggressivness = aggressivness_dict.get("aggressiveness")
        max_duration = aggressivness_dict.get("max_duration")

        self.chunking_processor.create_audio_clips(
            aggressivness,
            max_duration,
            wav_file_path,
            local_chunk_output_path,
            local_vad_output_path,
            file_name,
        )

        return local_chunk_output_path

    def _process_snr(
            self,
            input_file_path,
            meta_data_file_path,
            local_path,
            audio_id,
            hash_code):
        snr_config = self.audio_processor_config.get(SNR_CONFIG)

        self.snr_processor.fit_and_move(
            self._get_all_wav_in_path(input_file_path),
            meta_data_file_path,
            snr_config.get("max_snr_threshold", 15),
            local_path,
            audio_id,
            hash_code,
        )

    def _get_csv_in_path(self, path):
        all_csvs = glob.glob(f"{path}/*.csv")

        if len(all_csvs) < 1:
            return None

        return all_csvs[0]

    def _get_all_wav_in_path(self, path):
        return glob.glob(f"{path}/*.wav")
