import os
import glob
from audio_processing.constants import CONFIG_NAME, REMOTE_RAW_FILE, CHUNKING_CONFIG, SNR_CONFIG, REMOTE_PROCESSED_FILE_PATH,\
    MASTER_META_DATA_FILE_PATH,MASTER_META_DATA_DONE_FILE_PATH
from common.utils import get_logger

Logger = get_logger("Audio Processor")


class AudioProcessor:

    """
    Class for breaking a downloaded file into smaller chunks of
    audio files as well as filtering out files with more than an acceptable level
    of Sound to Noise Ratio(or SNR)
    """

    DEFAULT_DOWNLOAD_PATH = '/tmp/audio_processing_raw'

    @staticmethod
    def get_instance(data_processor, gcs_instance, audio_commons):
        return AudioProcessor(data_processor, gcs_instance, audio_commons)

    def __init__(self, data_processor, gcs_instance, audio_commons):
        self.data_processor = data_processor
        self.gcs_instance = gcs_instance
        self.snr_processor = audio_commons.get('snr_util')
        self.chunking_processor = audio_commons.get('chunking_conversion')
        self.audio_processor_config = None

    def process(self, **kwargs):
        """
        Function for breaking an audio file into smaller chunks and then
        accepting/rejecting them basis the SNR ratio.
        """
        self.audio_processor_config = self.data_processor.config_dict.get(
            CONFIG_NAME)

        audio_id_list = kwargs.get('audio_id_list', [])
        source = kwargs.get('source')
        extension = kwargs.get('extension')

        Logger.info(f'Processing audio ids {audio_id_list}')
        for audio_id in audio_id_list:
            Logger.info(f'Processing audio_id {audio_id}')
            self.process_audio_id(audio_id, source, extension)

    def process_audio_id(self, audio_id, source, extension):

        local_audio_download_path = f'{AudioProcessor.DEFAULT_DOWNLOAD_PATH}/{source}/{audio_id}'

        Logger.info(
            f'Downloading file for audio_id/{audio_id} to {local_audio_download_path}')

        self.ensure_path(local_audio_download_path)
        Logger.info(f'Ensured {local_audio_download_path} exists')

        master_metadat_file_path = f'{self.audio_processor_config.get(MASTER_META_DATA_FILE_PATH)}/{source}/{source}_master.csv'

        meta_data_file_exists = self.gcs_instance.check_path_exists(
            master_metadat_file_path)

        if meta_data_file_exists:
            self.upload_and_move(master_metadat_file_path,source)

        remote_file_path = self.audio_processor_config.get(REMOTE_RAW_FILE)
        remote_download_path = f'{remote_file_path}/{source}/{audio_id}'

        Logger.info(
            f'Downloading audio file from {remote_download_path} to {local_audio_download_path}')
        self.gcs_instance.download_to_local(self.gcs_instance.bucket, remote_download_path,
                                            local_audio_download_path, True)

        meta_data_file_path = self._get_csv_in_path(local_audio_download_path)

        Logger.info(f'Conerting the file with audio_id {audio_id} to wav')
        local_converted_wav_file_path = self._convert_to_wav(
            local_audio_download_path, extension)

        if not local_audio_download_path:
            return

        Logger.info(
            f'Breaking {audio_id} at {local_converted_wav_file_path} file into chunks')
        chunk_output_path = self._break_files_into_chunks(
            audio_id, local_audio_download_path, local_converted_wav_file_path)

        Logger.info(
            f'Processing SNR ratios for the all the chunks for audio_id {audio_id}')
        self._process_snr(chunk_output_path, meta_data_file_path,
                          local_audio_download_path, audio_id)

        processed_remote_file_path = self.audio_processor_config.get(
            REMOTE_PROCESSED_FILE_PATH)
        upload_path = f'{processed_remote_file_path}/{source}/{audio_id}'

        clean_file_upload = self.gcs_instance.upload_to_gcs(
            f'{local_audio_download_path}/clean', f'{upload_path}/clean')
        rejected_file_upload = self.gcs_instance.upload_to_gcs(
            f'{local_audio_download_path}/rejected', f'{upload_path}/rejected')

        if not clean_file_upload and not rejected_file_upload:
            Logger.error(
                f'Uploading chunked/snr cleaned files failed for {audio_id} not processing further.')

        self.upload_file(meta_data_file_path)

    def upload_and_move(self, master_metadat_file_path,source):

        meta_data_done_path = f'{self.audio_processor_config.get(MASTER_META_DATA_DONE_FILE_PATH)}/{source}'

        local_metadata_downloaded_path = '/tmp/master_csv'
        self.ensure_path(local_metadata_downloaded_path)

        self.gcs_instance.download_blob(
            master_metadat_file_path, local_metadata_downloaded_path, False)
        self.upload_file_to_downloaded_source(local_metadata_downloaded_path)

        self.gcs_instance.move_blob(master_metadat_file_path, meta_data_done_path)

    def ensure_path(self, path):
        # TODO: make path empty before creating it again
        os.makedirs(path, exist_ok=True)

    def _convert_to_wav(self, source_file_directory, extension):
        Logger.info(
            f'Converting the contents of the local path {source_file_directory} to wav')

        local_output_directory = f'{source_file_directory}/wav'

        self.ensure_path(local_output_directory)
        Logger.info(
            f'Output initialized to local path {local_output_directory}')

        output_file_path, converted = self.chunking_processor.convert_to_wav(
            source_file_directory, output_dir=local_output_directory, ext=extension)

        if not converted:
            return None

        return output_file_path

    def _break_files_into_chunks(self, audio_id, local_download_path, wav_file_path):

        Logger.info(f'Chunking audio file at {wav_file_path}')
        local_chunk_output_path = f'{local_download_path}/chunks'
        local_vad_output_path = f'{local_download_path}/vad'

        aggressivness_dict = self.audio_processor_config.get(CHUNKING_CONFIG, {'aggressiveness':2})

        if not isinstance(aggressivness_dict.get('aggressiveness'), int):
            raise Exception(f'Aggressiveness must be an int, not {aggressivness_dict}')

        self.ensure_path(local_chunk_output_path)
        Logger.info(f'Ensuring path {local_chunk_output_path}')
        file_name = wav_file_path.split('/')[-1]

        self.chunking_processor.create_audio_clips(aggressivness_dict.get('aggressiveness'), wav_file_path, local_chunk_output_path, local_vad_output_path, file_name)

        return local_chunk_output_path

    def _process_snr(self, input_file_path, meta_data_file_path, local_path, audio_id):
        snr_config = self.audio_processor_config.get(SNR_CONFIG)

        self.snr_processor.fit_and_move(self._get_all_wav_in_path(
            input_file_path), meta_data_file_path, snr_config.get('max_snr_threshold', 15), local_path, audio_id)

    def _get_csv_in_path(self, path):
        all_csvs = glob.glob(f'{path}/*.csv')

        if len(all_csvs) < 1:
            return None

        return all_csvs[0]

    def _get_all_wav_in_path(self, path):
        return glob.glob(f'{path}/*.wav')

    def upload_file(self, meta_data_path):
        """
        Uploading the meta data file from local to
        """

        db = self.data_processor.db

        with open(meta_data_path, 'r') as f:
            conn = db.raw_connection()
            cursor = conn.cursor()
            cmd = 'COPY media_metadata_staging(raw_file_name,duration,title,speaker_name,audio_id,cleaned_duration,num_of_speakers,language,has_other_audio_signature,type,source,experiment_use,utterances_files_list,source_url,speaker_gender,source_website,experiment_name,mother_tongue,age_group,recorded_state,recorded_district,recorded_place,recorded_date,purpose) FROM STDIN WITH (FORMAT CSV, HEADER)'
            cursor.copy_expert(cmd, f)
            conn.commit()

    def upload_file_to_downloaded_source(self, file_path):

        db_conn = self.data_processor.db

        Logger.info("uploading data to source_metadata")
        with open(file_path, 'r') as f:
            conn = db_conn.raw_connection()
            cursor = conn.cursor()
            cmd = 'COPY source_metadata_downloaded(source,num_speaker,total_duration,num_of_audio) FROM STDIN WITH (FORMAT CSV, HEADER)'
            cursor.copy_expert(cmd, f)
            conn.commit()
