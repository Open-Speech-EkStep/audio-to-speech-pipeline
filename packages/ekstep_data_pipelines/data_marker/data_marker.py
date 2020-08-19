from common.utils import get_logger
from data_marker.constants import CONFIG_NAME, SPEAKER_CRITERIA, SOURCE_CRITERIA, \
    FILTER_CRITERIA, NUMBER_OF_SPEAKERS, DURATION, SOURCE, \
    FILE_INFO_UPDATE_QUERY, LANDING_PATH, SOURCE_PATH, CONFIGERATION_DICT,\
    SELECT_SPEAKER_FOR_DATA_GREATER_THAN_DURATION_QUERY, FILE_INFO_QUERY, SOURCE_UPDATE_QUERY, SOURCE_NAME, SELECT_SPEAKER_FOR_DATA_GREATER_THAN_DURATION_WITH_SOURCE_QUERY,\
    PROCESS_MODE, SELECT_SPEAKER_FOR_DATA_LESS_THAN_DURATION_WITH_SOURCE_QUERY, SELECT_SPEAKER_FOR_DATA_LESS_THAN_DURATION_QUERY
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text
import sys
sys.path.insert(0, '..')


Logger = get_logger("Data marker")


class DataMarker:
    """
    1. Load Configeration
    2. Tag/Mark data in the DB
    3. Move marked data
    """

    @staticmethod
    def get_instance(data_processor_instance, gcs_instance):
        return DataMarker(data_processor_instance, gcs_instance)

    def __init__(self, data_processor_instance, gcs_instance):
        self.data_processor = data_processor_instance
        self.gcs_instance = gcs_instance
        self.data_tagger_config = None

    def process(self, **kwargs):
        """
        Main function for running all processing that takes places in the data marker
        """

        self.data_tagger_config = self.data_processor.config_dict.get(
            CONFIG_NAME)

        filter_criteria = self.data_tagger_config.get(FILTER_CRITERIA)
        landing_path = self.data_tagger_config.get(LANDING_PATH)
        source_path = self.data_tagger_config.get(SOURCE_PATH)

        if not filter_criteria:
            # TODO: Raise exception of misconfigeration
            return

        if filter_criteria.get(SPEAKER_CRITERIA):
            speaker_dict = self.get_speakers_with_source_duration(
                filter_criteria.get(SPEAKER_CRITERIA))
            Logger.info(
                f"All speaker list in given criteria is {speaker_dict}")
            file_move_info_list = self.process_file_info_update_query(
                speaker_dict)
            self._move_files(landing_path, source_path, file_move_info_list)

        if filter_criteria.get(SOURCE_CRITERIA):
            self.process_source_update_query(
                filter_criteria.get(SOURCE_CRITERIA))
            self._move_folder(filter_criteria,source_path,landing_path)

    def _move_folder(self,filter_criteria,source_path,landing_path):
        worker_pool = ThreadPoolExecutor(max_workers=3)

        source_list = filter_criteria.get(SOURCE_CRITERIA).get(SOURCE)

        for source in source_list:
            all_path = self.gcs_instance.list_blobs_in_a_path(f'{source_path}/{source}/')

        for path in all_path:
            full_path = path.name
        
            worker_pool.submit(self.copy_files,full_path, source,landing_path)
        
        worker_pool.shutdown(wait=True)

    def copy_files(self, source_file_path, source, landing_path):
        file_path_with_source = source_file_path.split('/')[-2:]
        join_path_source_file = '/'.join(file_path_with_source)

        destination_blob_name = f'{landing_path}/{join_path_source_file}'

        self.gcs_instance.move_blob(source_file_path,destination_blob_name)

    def _move_files(self, landing_path, source_path, file_move_info_list):
        worker_pool = ThreadPoolExecutor(max_workers=3)

        for file_info in file_move_info_list:
            file_path = file_info.get('source_file_path')
            meta_file_path = file_info.get('meta_data_source_file_path')

            source_file_path = f'{source_path}/{file_path}'
            dest_file_path = f'{landing_path}/{file_path}'

            source_meta_file_path = f'{source_path}/{meta_file_path}'
            dest_meta_file_path = f'{landing_path}/{meta_file_path}'
            Logger.info(f"Moving file {source_file_path} to {dest_file_path}")

            worker_pool.submit(self.gcs_instance.move_blob,
                               source_file_path, dest_file_path)
            worker_pool.submit(self.gcs_instance.move_blob,
                               source_meta_file_path, dest_meta_file_path)

        worker_pool.shutdown(wait=True)

    def process_source_update_query(self, source_filter_critieria):
        source_list = ",".join(
            [f"'{i}'" for i in source_filter_critieria.get(SOURCE)])
        final_query = f'{SOURCE_UPDATE_QUERY} ({source_list});'
        query = text(final_query)
        self.data_processor.connection.execute(query)

    def generate_query(self, process_mode, source_name):
        curr_config_dict = CONFIGERATION_DICT.get(process_mode)

        query_tuple = curr_config_dict.get('with_source')

        if not source_name:
            query_tuple = curr_config_dict.get('without_source')

        query_1, query_2 = query_tuple

        if query_1 != None:
            query_1 = text(query_1)

        if query_2 != None:
            query_2 = text(query_2)

        return query_1, query_2

    def _get_speaker_name_list(self, speaker_criteria):

        duration = speaker_criteria.get(DURATION)
        speaker_count = speaker_criteria.get(NUMBER_OF_SPEAKERS)
        source_name = speaker_criteria.get(SOURCE_NAME)
        process_mode = speaker_criteria.get(PROCESS_MODE)

        get_speaker_query, get_all_speaker = self.generate_query(
            process_mode, source_name)

        parm_dict = {}

        parm_dict["duration"] = duration
        parm_dict["speaker_count"] = speaker_count

        if source_name:
            parm_dict["source_name"] = source_name

        speakers = None

        if get_all_speaker != None:
            # get all the speakers
            speakers_greater_than_duration = self.data_processor.connection.execute(
                get_speaker_query, **parm_dict).fetchall()

            speakers_less_than_duration = self.data_processor.connection.execute(
                get_all_speaker, **parm_dict).fetchall()

            speakers = speakers_greater_than_duration + speakers_less_than_duration

        else:
            speakers = self.data_processor.connection.execute(
                get_speaker_query, **parm_dict).fetchall()

        if len(speakers) < 1:
            # TODO: Raise appropriate exception
            pass

        speaker_name_list = [
            f"'{self.escape_sql_special_char(speaker_name[0])}'" for speaker_name in speakers]
        formatted_name_list = ','.join(speaker_name_list)
        return f'({formatted_name_list})'

    def get_speakers_with_source_duration(self, speaker_criteria):
        duration = speaker_criteria.get(DURATION)
        speaker_count = speaker_criteria.get(NUMBER_OF_SPEAKERS)
        process_mode = speaker_criteria.get(PROCESS_MODE)

        if not all([duration, speaker_count, process_mode]):
            return None

        speaker_names = self._get_speaker_name_list(speaker_criteria)
        Logger.info(f"speaker name list is {speaker_names}")

        file_info_query_complete = f'{FILE_INFO_QUERY} {speaker_names};'
        Logger.info(f"find info query is {file_info_query_complete}")

        file_info_query = text(file_info_query_complete)
        file_info = self.data_processor.connection.execute(
            file_info_query).fetchall()

        return self._deduplicate_file_info(file_info, duration)

    def process_file_info_update_query(self, speaker_dict):
        file_list = []
        source_file_list = []

        for speaker in speaker_dict.keys():
            file_list = file_list + [i[1] for i in speaker_dict.get(speaker)]
            source_file_list = source_file_list + \
                [[i[0], i[1]] for i in speaker_dict.get(speaker)]

        file_list_with_single_quotes = [f"'{i}'" for i in file_list]
        source_list_name_query_param = f'({",".join(file_list_with_single_quotes)})'

        # format and run query
        final_file_update_query = f'{FILE_INFO_UPDATE_QUERY} {source_list_name_query_param}'
        Logger.info(f"Updated query for all files {final_file_update_query}")
        query = text(final_file_update_query)
        self.data_processor.connection.execute(query)

        return [self._get_file_path(i[0], i[1]) for i in source_file_list]

    def _get_file_path(self, source_name, file_name):
        source_info = {}

        file_name = self.clean_filename(file_name)

        meta_data_file_name = file_name.split('.')[0] + '.csv'
        source_info['meta_data_source_file_path'] = f'{source_name}/{meta_data_file_name}'

        if '.' not in file_name:
            source_path = self.data_tagger_config.get(SOURCE_PATH)
            file_path = f'{source_path}/{source_name}/{file_name}.mp4'
            exists = self.gcs_instance.check_path_exists(file_path)

            if not exists:
                file_name = f'{file_name}.mp3'
            else:
                file_name = f'{file_name}.mp4'

        source_info['source_file_path'] = f'{source_name}/{file_name}'

        return source_info

    def clean_filename(self,file_name):
        count_of_dot_occur = file_name.count(".")

        file_name = file_name.replace(' ','_')

        if count_of_dot_occur == 2:
            file_name = file_name.replace('.', '_', 1)
        
        return file_name

    def escape_sql_special_char(self, param):
        return param.replace("'", "''")

    def _deduplicate_file_info(self, file_info_list, duration):
        speaker_name_dict = {}
        speaker_duration_dict = {}

        for record in file_info_list:
            speaker_name = record[3]

            if not speaker_name in speaker_name_dict.keys():
                speaker_name_dict[speaker_name] = []
                speaker_duration_dict[speaker_name] = 0

            if speaker_duration_dict[speaker_name] >= duration:
                continue

            speaker_name_dict[speaker_name].append(record)
            speaker_duration_dict[speaker_name] = speaker_duration_dict[speaker_name] + record[2]

        return speaker_name_dict
