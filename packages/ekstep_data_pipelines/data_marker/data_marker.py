import multiprocessing

from common import CatalogueDao
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
        self.data_mover = MediaFilesMover(self.gcs_instance, multiprocessing.cpu_count() / 1 - ESTIMATED_BLOCKING_TIME_FRACTION)
        self.catalogue_dao = CatalogueDao(self.postgres_client)

    def process(self, **kwargs):
        """
        Main function for running all processing that takes places in the data marker
        """
        self.data_tagger_config = self.data_processor.config_dict.get(
            CONFIG_NAME)

        filter_criteria = self.data_tagger_config.get(FILTER_CRITERIA)
        landing_path = self.data_tagger_config.get(LANDING_PATH)
        source_path = self.data_tagger_config.get(SOURCE_PATH)
        source = filter_criteria['source']
        by_snr = filter_criteria['by_snr']
        by_speaker = filter_criteria['by_speaker']
        Logger.info("Fetching utterances for source:" + source)
        utterances = self.catalogue_dao.get_utterances_by_source(source, 'Clean')
        if by_snr is not None:
            Logger.info("Filtering by snr:" + str(by_snr))
            filtered_utterances = self.data_filter.by_snr(utterances, by_snr)
        elif by_speaker is not None:
            Logger.info("Filtering by speaker:" + str(by_speaker))
            filtered_utterances = self.data_filter.by_per_speaker_duration(utterances, by_speaker)
        else:
            raise Exception('filter criteria is not valid')
        Logger.info("updating utterances that need to be staged, count=" + len(filtered_utterances))
        self.catalogue_dao.update_utterances_staged_for_transcription(filtered_utterances)
        files = self.to_files(filtered_utterances, source_path, landing_path)
        Logger.info("Staging utterances......")
        self.data_mover.move_media_files(files, landing_path)

    def to_files(self, utterances, source_path):
        return list(map(lambda u: f'{source_path}/{u[3]}/clean/{u[1]}', utterances))
