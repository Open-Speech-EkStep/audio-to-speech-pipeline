import sys
sys.path.insert(0,'..')

import yaml
from data_marker.contstants import CONFIG_NAME, SPEAKER_CRITERIA, SOURCE_CRITERIA, \
                        FILTER_CRITERIA, NUMBER_OF_SPEAKERS, DURATION, SOURCE, \
                        FILE_INFO_UPDATE_QUERY, LANDING_PATH, SOURCE_PATH, \
                        SELECT_SPEAKER_QUERY, FILE_INFO_QUERY, SOURCE_UPDATE_QUERY
from sqlalchemy import create_engine, select, MetaData, Table, text



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

    def process(self):
        """
        Main function for running all processing that takes places in the data marker
        """

        self.data_tagger_config = self.data_processor.config_dict.get(CONFIG_NAME)

        filter_criteria = self.data_tagger_config.get(FILTER_CRITERIA)
        landing_path = self.data_tagger_config.get(LANDING_PATH)
        source_path = self.data_tagger_config.get(SOURCE_PATH)

        if not filter_criteria:
            # TODO: Raise exception of misconfigeration
            return


        if filter_criteria.get(SPEAKER_CRITERIA):
            speaker_dict = self.get_speakers_with_source_duration(filter_criteria.get(SPEAKER_CRITERIA))
            self.process_file_info_update_query(speaker_dict)
            self._move_files(landing_path, source_path)


        if filter_criteria.get(SOURCE_CRITERIA):
            self.process_source_update_query(filter_criteria.get(SOURCE_CRITERIA))

    def _move_files(self, landing_path, source_path):
        pass

    def process_source_update_query(self, source_filter_critieria):
        source_list = ",".join([f"'{i}'" for i in source_filter_critieria.get(SOURCE)])
        final_query = f'{SOURCE_UPDATE_QUERY} ({source_list});'
        query = text(final_query)
        self.data_processor.connection.execute(query)



    def _get_speaker_name_list(self, speaker_criteria):
        duration = speaker_criteria.get(DURATION)
        speaker_count = speaker_criteria.get(NUMBER_OF_SPEAKERS)

        # get all the speakers
        get_speaker_query = text(SELECT_SPEAKER_QUERY)
        speakers = self.data_processor.connection.execute(get_speaker_query, duration=duration, speaker_count=speaker_count).fetchall()

        if len(speakers) < 1:
            # TODO: Raise appropriate exception
            pass

        speaker_name_list = [f"'{speaker_name[0]}'" for speaker_name in speakers]
        formatted_name_list = ','.join(speaker_name_list)
        return f'({formatted_name_list})'

    def get_speakers_with_source_duration(self, speaker_criteria):
        duration = speaker_criteria.get(DURATION)
        speaker_count = speaker_criteria.get(NUMBER_OF_SPEAKERS)

        if not all([duration, speaker_count]):
            return None

        speaker_names = self._get_speaker_name_list(speaker_criteria)
        file_info_query_complete = f'{FILE_INFO_QUERY} {speaker_names};'
        file_info_query = text(file_info_query_complete)
        file_info = self.data_processor.connection.execute(file_info_query).fetchall()

        return self._deduplicate_file_info(file_info, duration)


    def process_file_info_update_query(self, speaker_dict):
        file_list = []

        for speaker in speaker_dict.keys():
            file_list = file_list + [i[1] for i in speaker_dict.get(speaker)]

        file_list_with_single_quotes = [f"'{i}'" for i in file_list]
        source_list_name_query_param = f'({",".join(file_list_with_single_quotes)})'

        final_file_update_query = f'{FILE_INFO_UPDATE_QUERY} {source_list_name_query_param}'
        query = text(final_file_update_query)
        return self.data_processor.connection.execute(query)

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