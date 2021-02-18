import multiprocessing

from ekstep_data_pipelines.common import CatalogueDao
from ekstep_data_pipelines.common.file_system.gcp_file_systen import GCPFileSystem
from ekstep_data_pipelines.common.utils import get_logger
from ekstep_data_pipelines.common import BaseProcessor


from ekstep_data_pipelines.data_marker.constants import (
    CONFIG_NAME,
    FILTER_CRITERIA,
    LANDING_BASE_PATH,
    SOURCE_BASE_PATH,
)
from ekstep_data_pipelines.data_marker.data_filter import DataFilter
from ekstep_data_pipelines.data_marker.data_mover import MediaFilesMover

ESTIMATED_CPU_SHARE = 0.02


Logger = get_logger("Data marker")


class DataMarker(BaseProcessor):
    """
    1. Load Configuration
    2. Filter data baased on criteria
    2. Tag/Mark data in the DB
    3. Move marked data
    """

    @staticmethod
    def get_instance(data_processor_instance, gcs_instance, **kwargs):
        return DataMarker(data_processor_instance, gcs_instance, **kwargs)

    def __init__(self, postgres_client, gcs_instance, **kwargs):
        self.postgres_client = postgres_client
        self.gcs_instance = gcs_instance
        self.data_tagger_config = None
        self.data_filter = DataFilter()
        Logger.info("Total available cpu count:" + str(multiprocessing.cpu_count()))
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
        source, filter_criteria = self.get_config(**kwargs)
        Logger.info("Fetching utterances for source:" + source)
        utterances = self.catalogue_dao.get_utterances_by_source(source, "Clean")

        filtered_utterances = self.data_filter.apply_filters(
            filter_criteria, utterances
        )
        Logger.info(
            "updating utterances that need to be staged, count="
            + str(len(filtered_utterances))
        )

        if len(filtered_utterances) > 0:
            rows_updated = (
                self.catalogue_dao.update_utterances_staged_for_transcription(
                    filtered_utterances, source
                )
            )
            Logger.info("Rows updated:" + str(rows_updated))
        else:
            Logger.info("No utterances found for filter criteria")
        source_dir = filter_criteria.get("landing_source_dir", source)
        landing_path_with_source = (
            f"{self.data_tagger_config.get(LANDING_BASE_PATH)}/{source_dir}"
        )
        source_path_with_source = (
            f"{self.data_tagger_config.get(SOURCE_BASE_PATH)}/{source}"
        )
        files = self.to_files(filtered_utterances, source_path_with_source)
        Logger.info("Staging utterances to dir:" + landing_path_with_source)
        self.data_mover.move_media_files(files, landing_path_with_source)
        Logger.info("************* Data marker completed ****************")

    def to_files(self, utterances, source_path_with_source):
        list(
            map(lambda u: f"{source_path_with_source}/{u[3]}/clean/{u[1]}", utterances)
        )
        return list(
            map(lambda u: f"{source_path_with_source}/{u[3]}/clean/{u[1]}", utterances)
        )

    def get_config(self, **kwargs):
        filter_criteria = kwargs.get(FILTER_CRITERIA, {})
        source = kwargs.get("source")

        if source is None:
            raise Exception("filter by source is mandatory")

        return source, filter_criteria.get(FILTER_CRITERIA)
