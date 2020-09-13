import multiprocessing

from common import CatalogueDao
from common.file_system.gcp_file_systen import GCPFileSystem
from common.utils import get_logger
import sys

from data_marker.constants import CONFIG_NAME, FILTER_CRITERIA, LANDING_PATH, SOURCE_PATH
from data_marker.data_filter import DataFilter
from data_marker.data_mover import MediaFilesMover

ESTIMATED_BLOCKING_TIME_FRACTION = .2

sys.path.insert(0, '..')

Logger = get_logger("Data marker")


class DataMarker:
    """
    1. Load Configuration
    2. Filter data baased on criteria
    2. Tag/Mark data in the DB
    3. Move marked data
    """

    @staticmethod
    def get_instance(data_processor_instance, gcs_instance):
        return DataMarker(data_processor_instance, gcs_instance)

    def __init__(self, postgres_client, gcs_instance):
        self.postgres_client = postgres_client
        self.gcs_instance = gcs_instance
        self.data_tagger_config = None
        self.data_filter = DataFilter()
        self.data_mover = MediaFilesMover(GCPFileSystem(self.gcs_instance),
                                          multiprocessing.cpu_count() / 1 - ESTIMATED_BLOCKING_TIME_FRACTION)
        self.catalogue_dao = CatalogueDao(self.postgres_client)

    def process(self, **kwargs):
        """
        Main function for running all processing that takes places in the data marker
        """

        Logger.info('*************Starting data marker****************')
        self.data_tagger_config = self.postgres_client.config_dict.get(
            CONFIG_NAME)

        filter_criteria = self.data_tagger_config.get(FILTER_CRITERIA)
        landing_path = self.data_tagger_config.get(LANDING_PATH)
        source_path = self.data_tagger_config.get(SOURCE_PATH)
        source = filter_criteria.get('by_source', None)
        if source is None:
            raise Exception('filter by source is mandatory')
        Logger.info("Fetching utterances for source:" + source)
        utterances = self.catalogue_dao.get_utterances_by_source(source, 'Clean')
        filtered_utterances = self.data_filter.apply_filters(filter_criteria, utterances)
        Logger.info("updating utterances that need to be staged, count=" + str(len(filtered_utterances)))
        self.catalogue_dao.update_utterances_staged_for_transcription(filtered_utterances)
        files = self.to_files(filtered_utterances, source_path)
        Logger.info("Staging utterances......")
        self.data_mover.move_media_files(files, landing_path)
        Logger.info('************* Data marker completed ****************')


    def to_files(self, utterances, source_path):
        return list(map(lambda u: f'{source_path}/{u[3]}/clean/{u[1]}', utterances))
