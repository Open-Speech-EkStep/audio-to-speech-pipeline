import multiprocessing
import os
from ekstep_data_pipelines.common import BaseProcessor
from ekstep_data_pipelines.common import CatalogueDao
from ekstep_data_pipelines.common.file_system.gcp_file_systen import GCPFileSystem
from ekstep_data_pipelines.common.utils import get_logger
from ekstep_data_pipelines.common.file_utils import *
from ekstep_data_pipelines.data_marker.constants import (
    CONFIG_NAME,
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
        source, data_set, filter_criteria, file_mode, file_path = self.get_config(**kwargs)
        if file_mode.lower() == 'y':
            Logger.info("Fetching already filtered utterances from a file for source: %s", source)
            ensure_path(self.local_input_path)
            download_path = self.download_filtered_utterances_file(file_path, self.local_input_path)
            filtered_utterances = self.get_utterances_from_file(download_path)
            source_dir = source
        else:
            Logger.info("Fetching utterances for source: %s", source)
            utterances = self.catalogue_dao.get_utterances_by_source(source, "Clean")
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
        else:
            Logger.info("No utterances found for filter criteria")

        Logger.info("************* Data marker completed ****************")

    def to_files(self, utterances, source_path_with_source):
        list(
            map(lambda u: f"{source_path_with_source}/{u[3]}/clean/{u[1]}", utterances)
        )
        return list(
            map(lambda u: f"{source_path_with_source}/{u[3]}/clean/{u[1]}", utterances)
        )

    def get_config(self, **kwargs):
        filter_spec = kwargs.get(FILTER_SPEC, {})
        filters = filter_spec.get(FILTER_CRITERIA, {})
        source = kwargs.get("source")
        file_mode = filter_spec.get(FILE_MODE)
        file_path = filter_spec.get(FILE_PATH)
        data_set = filter_spec.get(DATA_SET)

        return source, data_set, filters, file_mode, file_path

    def download_filtered_utterances_file(self, input_file_path, local_path):

        Logger.info(
            f"Downloading file from path from {input_file_path}"
        )
        download_path = f'{local_path}{os.path.basename(input_file_path)}'
        self.fs_interface.download_file_to_location(
            input_file_path, download_path
        )

        if check_file_exits(download_path):
            Logger.info("File Downloaded successfully")
        else:
            raise Exception("File Download failed")
        return download_path

    def get_utterances_from_file(self, local_file_path):
        df = pd.read_csv(local_file_path)
        return list(df[FILE_COLUMN_LIST].to_records(index=False, column_dtypes={"speaker_id": "int32"}))
