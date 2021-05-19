import multiprocessing
import os
from ekstep_data_pipelines.common import BaseProcessor
from ekstep_data_pipelines.common import CatalogueDao
from ekstep_data_pipelines.common.file_system.gcp_file_systen import GCPFileSystem
from ekstep_data_pipelines.common.utils import get_logger
from ekstep_data_pipelines.common.file_utils import *
from ekstep_data_pipelines.data_marker.constants import (
    CONFIG_NAME,
    COMMON_CONFIG_NAME,
    COMMON_GCS_CONFIG_NAME,
    COMMON_GCS_BUCKET_CONFIG,
    FILTER_CRITERIA,
    LANDING_BASE_PATH,
    SOURCE_BASE_PATH,
    FILE_MODE,
    FILE_PATH,
    FILE_COLUMN_LIST,
    FILTER_SPEC,
    DATA_SET
)
from ekstep_data_pipelines.data_marker.data_filter import DataFilter
from ekstep_data_pipelines.data_marker.data_mover import MediaFilesMover
import pandas as pd

ESTIMATED_CPU_SHARE = 0.02

Logger = get_logger("Data marker")


class DataMarker(BaseProcessor):
    """
    1. Load Configuration
    2. Filter data baased on criteria
    2. Tag/Mark data in the DB
    3. Move marked data
    """
    local_input_path = "./data_marker/file_path/"

    @staticmethod
    def get_instance(data_processor_instance, gcs_instance, **kwargs):
        return DataMarker(data_processor_instance, gcs_instance, **kwargs)

    def __init__(self, postgres_client, gcs_instance, **kwargs):
        self.postgres_client = postgres_client
        self.gcs_instance = gcs_instance
        self.data_tagger_config = None
        self.data_filter = DataFilter()
        Logger.info("Total available cpu count: %s", str(multiprocessing.cpu_count()))
        self.data_mover = MediaFilesMover(
            GCPFileSystem(self.gcs_instance),
            multiprocessing.cpu_count() / ESTIMATED_CPU_SHARE,
        )
        self.catalogue_dao = CatalogueDao(self.postgres_client)

        super().__init__(**kwargs)

    def process(self, **kwargs):
        """
        Main function for running all processing that takes places in the data marker
        """
        Logger.info("*************Starting data marker****************")
        self.data_tagger_config = self.postgres_client.config_dict.get(CONFIG_NAME)
        self.bucket = self.postgres_client.config_dict.get(COMMON_CONFIG_NAME).get(COMMON_GCS_CONFIG_NAME).get(
            COMMON_GCS_BUCKET_CONFIG)
        source, data_set, filter_criteria, file_mode, file_path = self.get_config(**kwargs)
        if file_mode.lower() == 'y':
            Logger.info("Fetching already filtered utterances from a file for source: %s", source)
            ensure_path(self.local_input_path)
            download_path = self.download_filtered_utterances_file(self.bucket, file_path, self.local_input_path)
            if check_file_exits(download_path):
                Logger.info("File Downloaded successfully")
                filtered_utterances = self.get_utterances_from_file(download_path)
                source_dir = source
            else:
                raise Exception("File Download failed")
        else:
            Logger.info("Fetching utterances for source: %s", source)
            utterances = self.catalogue_dao.get_utterances_by_source(source, "Clean", data_set)
            Logger.info("Applying filters on %d utterances for source: %s", len(utterances), source)
            filtered_utterances = self.data_filter.apply_filters(
                filter_criteria, utterances
            )
            source_dir = filter_criteria.get("landing_source_dir", source)
        Logger.info(
            "updating utterances that need to be staged, count=%s",
            str(len(filtered_utterances)),
        )

        landing_path_with_source = (
            f"{self.data_tagger_config.get(LANDING_BASE_PATH)}/{source_dir}/{data_set}"
        )
        source_path_with_source = (
            f"{self.data_tagger_config.get(SOURCE_BASE_PATH)}/{source}"
        )
        files = self.to_files(filtered_utterances, source_path_with_source)
        Logger.info("Staging utterances to dir: %s", landing_path_with_source)
        self.data_mover.move_media_files(files, landing_path_with_source)

        if len(filtered_utterances) > 0:
            rows_updated = (
                self.catalogue_dao.update_utterances_staged_for_transcription(
                    filtered_utterances, source, data_set
                )
            )
            Logger.info("Rows updated: %s", str(rows_updated))
            dictinct_audio_ids = self.fetch_distinct_audio_ids(filtered_utterances)
            Logger.info("Updating audio_ids with data set type used for tags")
            self.catalogue_dao.update_audio_ids_with_data_type(source, dictinct_audio_ids, data_set)
            Logger.info("All audio_ids updated with data set type tags")
            # Data archival
            # paths = self.to_paths(dictinct_audio_ids, source_path_with_source)
            # archive_path_with_source = (
            #     f"{self.data_tagger_config.get(SOURCE_BASE_PATH)}/{source}/archive"
            # )
            # Logger.info("Archiving audio_ids to dir: %s", archive_path_with_source)
            # self.data_mover.move_media_paths(paths, archive_path_with_source)
        else:
            Logger.info("No utterances found for filter criteria")

        Logger.info("************* Data marker completed ****************")

    def to_files(self, utterances, source_path_with_source):
        """Returns the list of complete path of utterances.

        Args:
            utterances: list of utterance files data
            source_path_with_source: base path for the audio source directory

        Returns:
            the list of complete path for provided utterances
        """
        list(
            map(lambda u: f"{source_path_with_source}/{u[3]}/clean/{u[1]}", utterances)
        )
        return list(
            map(lambda u: f"{source_path_with_source}/{u[3]}/clean/{u[1]}", utterances)
        )

    def to_paths(self, audio_ids, source_path_with_source):
        """Returns the list of complete path of audio.

        Args:
            audio_ids: list of audio_ids
            source_path_with_source: base path for the audio source directory

        Returns:
            the list of complete path for provided utterances
        """
        return list(
            map(lambda a: f"{source_path_with_source}/{a}", audio_ids)
        )

    def get_config(self, **kwargs):
        """Extract configuration values for filter_spec from given json data.

        Args:
            kwargs: contains key-value pair in json format for filter_spec

        Returns:
            Returns source, data_set, filters, file_mode, file_path

        """

        filter_spec = kwargs.get(FILTER_SPEC, {})
        filters = filter_spec.get(FILTER_CRITERIA, {})
        source = kwargs.get("source")
        file_mode = filter_spec.get(FILE_MODE)
        file_path = filter_spec.get(FILE_PATH)
        data_set = filter_spec.get(DATA_SET)

        return source, data_set, filters, file_mode, file_path

    def download_filtered_utterances_file(self, bucket, input_file_path, local_path):
        """Download the filtered utterance file from input_file_path to local_path

        Args:
            bucket: bucket name
            input_file_path: path of the filtered utterances csv file
            local_path: local directory path where file needs to be downloaded

        Returns:
            Download path where filtered utterance file is downloaded.

        """
        input_file_path_abs = bucket + '/' + input_file_path
        Logger.info(
            f"Downloading file from path from {input_file_path_abs}"
        )
        download_path = f'{local_path}{os.path.basename(input_file_path_abs)}'
        self.fs_interface.download_file_to_location(
            input_file_path_abs, download_path
        )

        return download_path

    def get_utterances_from_file(self, local_file_path):
        """Returns utterance list from csv file, if file is empty raise an exception.

        Args:
            local_file_path: file path of csv file containing utterance records.

        Returns:
            Complete download path

        Raises:
            Exception: Empty file with no records.
        """
        df = pd.read_csv(local_file_path)
        if not df.empty:
            return list(df[FILE_COLUMN_LIST].to_records(index=False, column_dtypes={"speaker_id": "int32"}))
        else:
            raise Exception("Empty filtered csv with no records..Aborting.")

    def fetch_distinct_audio_ids(self, utterances):
        """Returns List of unique audio_ids from utterance list

        Args:
            utterances: list of utterance files data

        Returns:
            List of unique audio_ids
        """
        df = pd.DataFrame.from_records(utterances, columns=FILE_COLUMN_LIST)
        return list(df['audio_id'].unique())
