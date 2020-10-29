import json
from common.utils import get_logger
from common import BaseProcessor

from normalizer.constants import MAX_LOAD_DATE_FOR_MEDIA_QUERY,INSERT_INTO_MEDIA_TABLE_QUERY,INSERT_UNIQUE_SPEAKER_QUERY,GET_AUDIO_ID_QUERY,\
    DEFULT_QUERY_FOR_INSERT_INTO_MAPPING_TABLE,GET_SPEAKER_ID_QUERY,FETCH_QUERY_WHERE_SPEAKER_IS_NULL,DEFAULT_INSERT_QUERY,DEFAULT_UPDATE_QUERY_FOR_NORMALIZED_FLAG,\
        GET_LOAD_TIME_FOR_AUDIO_QUERY,GET_UTTERANCES_LIST_OF_AUDIO_ID


Logger = get_logger("Normalizer")


class Normalizer(BaseProcessor):
    """
    docstring
    """
    @staticmethod
    def get_instance(data_processor):
        return Normalizer(data_processor)

    def __init__(self,data_processor):

        self.data_processor = data_processor

    def process(self, **kwargs):
        Logger.info('Normalizer utility')
        """
        Function for breaking an audio file into smaller chunks and then
        accepting/rejecting them basis the SNR ratio.
        """
        Logger.info("moving data from staging to mapping table if speaker is not identified")
        self.update_mapping_table_when_speaker_is_null()

        Logger.info("moving data from staging to media......")
        self.copy_data_from_media_metadata_staging_to_media()

        Logger.info("moving data from staging to media done")
        Logger.info("moving data from staging to speaker......")
        self.copy_data_from_media_metadata_staging_to_speaker()

        Logger.info("moving data from staging to speaker done")
        Logger.info("moving data from staging to media_speaker_mapping ....")
        self.copy_data_media_speaker_mapping()

        Logger.info("moving data from staging to media_speaker_mapping done")

    def update_mapping_table_when_speaker_is_null(self):

        insert_query, processed_audio_ids = self.update_utterance_in_mapping_table()

        if len(insert_query) < 1:
            # TODO: should raise execption
            return

        default_query = DEFAULT_INSERT_QUERY

        final_query = default_query + ','.join(insert_query)

        self.data_processor.execute_update(final_query)

        self.set_isnormalized_flag(
            processed_audio_ids)

    def set_isnormalized_flag(self, audio_ids,is_nested=False):

        if len(audio_ids) <= 0:
            Logger.info("No file found where speaker is null")
            return
        
        audio_id_list = f"({','.join([f'{audio_id}' for audio_id in audio_ids])})"

        if is_nested:
            audio_id_list = f"({','.join([f'{audio_id[0]}' for audio_id in audio_ids])})"

        query = DEFAULT_UPDATE_QUERY_FOR_NORMALIZED_FLAG + audio_id_list

        self.data_processor.execute_update(query)

    def update_utterance_in_mapping_table(self):

        all_data = self.data_processor.execute_query(FETCH_QUERY_WHERE_SPEAKER_IS_NULL)

        processed_audio_ids = []

        insert_query_into_mapping_table = []

        for onefile in all_data:
            audio_id = onefile[0]
            load_datetime = onefile[2]

            utterance_list = self.parse_raw_file_data(onefile[1])

            processed_audio_ids.append(audio_id)

            if utterance_list == None:
                Logger.info(audio_id)
                continue

            for utterance in utterance_list:
                
                if str(utterance.get('snr_value')) == 'nan':
                    snr_value = 0.0
                else:
                    snr_value = float(utterance.get('snr_value', 0))

                insert_query_into_mapping_table.append(f"('{utterance['name']}',{utterance['duration']},\
                    {audio_id},{snr_value},'{utterance['status']}','{utterance.get('reason','')}','{load_datetime}')")

        return insert_query_into_mapping_table, processed_audio_ids

    def parse_raw_file_data(self, raw_file_utterance):
        try:
            data = json.loads(raw_file_utterance)
            return data
        except json.decoder.JSONDecodeError as error:
            Logger.info(error)
            return raw_file_utterance

    def copy_data_from_media_metadata_staging_to_media(self):
        max_date_result = self.data_processor.execute_query(MAX_LOAD_DATE_FOR_MEDIA_QUERY)
        max_datetime = max_date_result[0][0]
        self.data_processor.execute_update(INSERT_INTO_MEDIA_TABLE_QUERY,
                           max_datetime=max_datetime)

    def copy_data_from_media_metadata_staging_to_speaker(self):
        self.data_processor.execute_update(INSERT_UNIQUE_SPEAKER_QUERY)

    def find_speaker_id(self, audio_id):
        results = self.data_processor.execute_query(
            GET_SPEAKER_ID_QUERY, audio_id=audio_id[0])

        if len(results) <=0:
            Logger.info("No sperakr id found in given audio_id")
            pass

        speaker_id = results[0][0]
        return speaker_id

    def get_load_datetime(self, audio_id):
        results = self.data_processor.execute_query(
            GET_LOAD_TIME_FOR_AUDIO_QUERY, audio_id=audio_id[0])
        if len(results) <= 0:
            Logger.info("All data normalized")
            return []
        date_time = results[0][0]
        return date_time

    def get_utterance_list(self, audio_id):

        utterance = self.data_processor.execute_query(
            GET_UTTERANCES_LIST_OF_AUDIO_ID, audio_id=audio_id[0])

        utterance_in_array = self.parse_raw_file_data(utterance[0][0])
        return utterance_in_array
    
    def copy_data_media_speaker_mapping(self):

        audio_ids = self.data_processor.execute_query(GET_AUDIO_ID_QUERY)

        defult_query = DEFULT_QUERY_FOR_INSERT_INTO_MAPPING_TABLE

        for audio_id in audio_ids:

            speaker_id = self.find_speaker_id(audio_id)
            get_load_datetime_for_audio = self.get_load_datetime(audio_id)

            if len(get_load_datetime_for_audio) <= 0:
                return

            utterance_list = self.get_utterance_list(audio_id)

            for utterance_name_diration in utterance_list:

                updated_query = self.create_insert_query(utterance_name_diration, speaker_id, audio_id, get_load_datetime_for_audio,defult_query)

                defult_query = updated_query

        if defult_query == DEFULT_QUERY_FOR_INSERT_INTO_MAPPING_TABLE:
            Logger.info("no record found")
            return

        final_query = defult_query[:-1]

        self.data_processor.execute_update(final_query)

        self.set_isnormalized_flag(audio_ids,True)


    def create_insert_query(self, utterance, speaker_id, audio_id, datetime,defult_query):

        file_name = utterance['name']
        duration = utterance['duration']
        status = utterance['status']
        fail_reason = utterance.get('reason', '')
        language_confidence_score = json.dumps(utterance.get('language_confidence_score', None))

        if str(utterance.get('snr_value')) == 'nan':
            snr_value = 0.0
        else:
            snr_value = float(utterance.get('snr_value', 0))

        Logger.info("inserting with language_confidence_score:" + str(language_confidence_score))

        return f"{defult_query} ({audio_id[0]},{speaker_id},'{file_name}',{duration},'{datetime}',{snr_value},'{status}','{fail_reason}','{language_confidence_score}'),"

    