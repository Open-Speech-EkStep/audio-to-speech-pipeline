import multiprocessing
import os
import sys

from ekstep_data_pipelines.audio_analysis.analyse_gender import analyse_gender
from ekstep_data_pipelines.audio_analysis.analyse_speaker import analyse_speakers
from ekstep_data_pipelines.audio_analysis.constants import (
    CONFIG_NAME,
    REMOTE_PROCESSED_FILE_PATH,
    AUDIO_ANALYSIS_PARAMS,
    ANALYSIS_OPTIONS,
)
from ekstep_data_pipelines.audio_analysis.speaker_analysis.create_embeddings import (
    encode_on_partial_sets, )
from ekstep_data_pipelines.common import BaseProcessor, CatalogueDao
from ekstep_data_pipelines.common.utils import get_logger

MIN_SAMPLES = 1

PARTIAL_SET_SIZE = 11122

MIN_CLUSTER_SIZE = 5

FIT_NOISE_ON_SIMILARITY = 0.80

ESTIMATED_CPU_SHARE = 0.1

LOGGER = get_logger("AudioSpeakerClusteringProcessor")


class AudioAnalysis(BaseProcessor):
    """
    Class to identify speaker for each utterance in a source
    """

    DEFAULT_DOWNLOAD_PATH = "./audio_speaker_cluster"

    @staticmethod
    def get_instance(data_processor, **kwargs):
        return AudioAnalysis(data_processor, **kwargs)

    def __init__(self, data_processor, **kwargs):
        self.data_processor = data_processor
        self.audio_analysis_config = None
        self.catalogue_dao = CatalogueDao(self.data_processor)

        # signal.signal(signal.SIGINT, self.handle_termination_gracefully)
        # signal.signal(signal.SIGTERM, self.handle_termination_gracefully)
        # signal.signal(signal.SIGKILL, self.handle_termination_gracefully)

        super().__init__(**kwargs)

    def handle_termination_gracefully(self, signum, frame):
        LOGGER.info(
            "SIGINT/SIGTERM invoked with the following information %f/%f", signum, frame
        )
        sys.exit(1)

    def process(self, **kwargs):
        """
        Function for mapping utterance to speakers
        """

        self.audio_analysis_config = self.data_processor.config_dict.get(
            CONFIG_NAME)

        source = self.get_source_from_config(**kwargs)
        parameters = self.get_speaker_analysis_params()

        embed_file_path = (
            f"{AudioAnalysis.DEFAULT_DOWNLOAD_PATH}/{source}_embed_file.npz"
        )
        local_audio_download_path = f"{AudioAnalysis.DEFAULT_DOWNLOAD_PATH}/{source}/"
        self.ensure_path(local_audio_download_path)

        LOGGER.info("Ensured %s exists", local_audio_download_path)
        remote_download_path = self.get_full_path(source)

        LOGGER.info("Total available cpu count: %s",
                    str(multiprocessing.cpu_count()))

        LOGGER.info(
            "Running speaker clustering using parameters: %s",
            str(parameters))
        min_cluster_size = parameters.get("min_cluster_size", MIN_CLUSTER_SIZE)
        partial_set_size = parameters.get("partial_set_size", PARTIAL_SET_SIZE)
        min_samples = parameters.get("min_samples", MIN_SAMPLES)
        fit_noise_on_similarity = parameters.get(
            "fit_noise_on_similarity", FIT_NOISE_ON_SIMILARITY
        )
        npz_destination_path = f"{remote_download_path}/{source}_embed_file.npz"

        analysis_options = self.get_analysis_options()

        speaker_to_file_name = None
        file_to_speaker_gender_mapping = None

        self.create_or_fetch_embeddings(
            local_audio_download_path,
            remote_download_path,
            embed_file_path,
            npz_destination_path,
            partial_set_size
        )

        if analysis_options.get("speaker_analysis") == 1:
            speaker_to_file_name = analyse_speakers(
                embed_file_path,
                source,
                min_cluster_size,
                partial_set_size,
                min_samples,
                fit_noise_on_similarity,
            )

        if analysis_options.get("gender_analysis") == 1:
            file_to_speaker_gender_mapping = analyse_gender(embed_file_path)

        self.update_info_in_db(
            self.catalogue_dao,
            speaker_to_file_name,
            file_to_speaker_gender_mapping,
            source,
        )

    def create_or_fetch_embeddings(
            self,
            local_audio_download_path,
            remote_download_path,
            embed_file_path,
            npz_bucket_destination_path,
            partial_set_size,
            dir_pattern="*.wav"
    ):
        if self.fs_interface.path_exists(npz_bucket_destination_path):
            self.fs_interface.download_file_to_location(
                npz_bucket_destination_path, embed_file_path
            )
        else:
            LOGGER.info(
                "Downloading source to $s from %s",
                local_audio_download_path, remote_download_path)
            self.fs_interface.download_folder_to_location(
                remote_download_path, local_audio_download_path, 5
            )
            # encoder(local_audio_download_path, dir_pattern, embed_file_path)
            encode_on_partial_sets(
                local_audio_download_path,
                dir_pattern,
                embed_file_path,
                partial_set_size)
            is_uploaded = self.fs_interface.upload_to_location(
                embed_file_path, npz_bucket_destination_path
            )
            if is_uploaded:
                LOGGER.info(
                    "npz file uploaded to : %s",
                    npz_bucket_destination_path)
            else:
                LOGGER.info(
                    "npz file could not be uploaded to : %s",
                    npz_bucket_destination_path)

    def update_info_in_db(
            self,
            catalogue_dao,
            speaker_to_file_name,
            file_to_speaker_gender_mapping,
            source,
    ):

        if speaker_to_file_name:
            self._update_speaker_count_info(
                catalogue_dao, speaker_to_file_name, source)

        if file_to_speaker_gender_mapping:
            self._update_speaker_gender_mapping(
                catalogue_dao, file_to_speaker_gender_mapping
            )

    def _update_speaker_gender_mapping(
            self, catalogue_dao, file_speaker_gender_mapping
    ):
        male_files = []
        female_files = []

        for file, gender in file_speaker_gender_mapping.items():

            utterance_name = file.split("/")[-1]

            if gender == "m":
                male_files.append(utterance_name)
            else:
                female_files.append(utterance_name)

        catalogue_dao.update_utterance_speaker_gender(male_files, "m")
        LOGGER.info(
            "Updating the %s with the value with value male", male_files)

        catalogue_dao.update_utterance_speaker_gender(female_files, "f")
        LOGGER.info(
            "Updating the %s with the value with value Female", female_files)

    def _update_speaker_count_info(
            self,
            catalogue_dao,
            speaker_to_file_name,
            source):
        for speaker in speaker_to_file_name:
            speaker_id = catalogue_dao.select_speaker(speaker, source)

            if speaker_id == -1:
                speaker_inserted = catalogue_dao.insert_speaker(
                    source, speaker)
            else:
                LOGGER.info("Speaker already exists:%s", speaker)
                speaker_inserted = True

            if not speaker_inserted:
                # do nothing incase the speaker_inserted is false
                continue

            LOGGER.info("updating utterances for speaker:%s", speaker)
            utterances = speaker_to_file_name.get(speaker)
            LOGGER.info("utterances: %s", str(utterances))

            def to_file_name(utterance):
                return utterance[0]

            was_noise_utterances = list(
                map(to_file_name, (filter(lambda u: u[1] == 1, utterances)))
            )
            fitted_utterances = list(
                map(to_file_name, (filter(lambda u: u[1] == 0, utterances)))
            )
            if len(was_noise_utterances) > 0:
                catalogue_dao.update_utterance_speaker(
                    was_noise_utterances, speaker, True
                )
            if len(fitted_utterances) > 0:
                catalogue_dao.update_utterance_speaker(
                    fitted_utterances, speaker, False
                )

    def get_full_path(self, source):
        remote_file_path = self.audio_analysis_config.get(
            REMOTE_PROCESSED_FILE_PATH)
        remote_download_path = f"{remote_file_path}/{source}"
        return remote_download_path

    def ensure_path(self, path):
        os.makedirs(path, exist_ok=True)

    def get_source_from_config(self, **kwargs):
        source = kwargs.get("source")

        if source is None:
            raise Exception("filter by source is mandatory")

        return source

    def get_speaker_analysis_params(self):
        return self.audio_analysis_config.get(AUDIO_ANALYSIS_PARAMS)

    def get_analysis_options(self):
        return self.audio_analysis_config.get(ANALYSIS_OPTIONS)
