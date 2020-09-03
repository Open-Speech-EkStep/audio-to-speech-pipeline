import os
import json
import ast
import sys
import yaml

from .db_query import MAX_LOAD_DATE_FOR_MEDIA_QUERY, INSERT_INTO_MEDIA_TABLE_QUERY, GET_SPEAKER_ID_QUERY, GET_LOAD_TIME_FOR_AUDIO_QUERY,\
     FIND_MAX_LOAD_DATE_QUERY, GET_AUDIO_ID_QUERY, INSERT_UNIQUE_SPEAKER_QUERY,GET_NEW_SOURCE_DATA_QUERY,UPDATE_SOURCE_METADATA_QUERY,INSERT_INTO_SOURCE_METADATA_QUERY,\
         FETCH_AND_UPDATE_QUERY_WHERE_SPEAKER_IS_NULL,DEFAULT_INSERT_QUERY

from os.path import join, dirname
from sqlalchemy import create_engine, select, MetaData, Table, text
from .gcs_operations import CloudStorageOperations



# def get_max_date(table_name,connection):
#     get_max_date = text("SELECT MAX (load_datetime) FROM media;")
#     max_date_result = connection.execute(get_max_date).fetchall()
#     max_datetime = max_date_result[0][0]

class Db_normalizer():

    def copy_data_from_media_metadata_staging_to_media(self,db):
        connection = db.connect()
        trans = connection.begin()
        try:
            get_max_date = text(MAX_LOAD_DATE_FOR_MEDIA_QUERY)
            max_date_result = connection.execute(get_max_date).fetchall()
            max_datetime = max_date_result[0][0]
            insert_into_media_table_from_staging = text(
                INSERT_INTO_MEDIA_TABLE_QUERY)
            connection.execute(insert_into_media_table_from_staging,
                            max_datetime=max_datetime)
            trans.commit()
        except:
            trans.rollback()
            raise


    def copy_data_from_media_metadata_staging_to_speaker(self,connection):
        # connection = db.connect()
        # trans = connection.begin()
        try:
            connection.execute(INSERT_UNIQUE_SPEAKER_QUERY)
            # trans.commit()
        except:
            # trans.rollback()
            raise
        # connection.commit()


    def find_speaker_id(self,connection, audio_id):
        # get_speaker_id = text("select speaker_id from speaker s JOIN media_metadata_staging b on s.speaker_name = b.speaker_name \
        #         where b.audio_id = :audio_id")
        get_speaker_id = text(GET_SPEAKER_ID_QUERY)
        results = connection.execute(
            get_speaker_id, audio_id=audio_id[0]).fetchall()
        speaker_id = results[0][0]
        return speaker_id


    def insert_file(self,connection,file_name):
        with open(file_name, 'r') as myfile:
            content = myfile.read()
            clear_content = content[:-1]
            # connection = db.connect()
            connection.execute(clear_content)
            print("insertion done")


    def create_insert_query(self,utterance, speaker_id, audio_id, datetime, connection):

        file_name = utterance['name']
        durtion = utterance['duration']
        snr = utterance['snr_value']
        status = utterance['status']
        fail_reason = utterance['reason']

        # print(utterance)
        with open("full_query.txt", 'a') as myfile:
            myfile.write(
                f"({audio_id[0]},{speaker_id},'{file_name}',{durtion},'{datetime}',{snr},{status},{fail_reason}),")


    def get_load_datetime(self,audio_id, connection):
        load_date_time_for_audio = text(GET_LOAD_TIME_FOR_AUDIO_QUERY)
        results = connection.execute(
            load_date_time_for_audio, audio_id=audio_id[0]).fetchall()
        date_time = results[0][0]
        return date_time


    def get_load_date_for_mapping(self,connection):
        results = connection.execute(FIND_MAX_LOAD_DATE_QUERY).fetchall()
        max_date = results[0][0]
        return max_date


    def get_utterance_list(self,connection, audio_id):
        utterances_list = text(
            "select utterances_files_list from media where audio_id = :audio_id")
        utterance = connection.execute(
            utterances_list, audio_id=audio_id[0]).fetchall()

        utterance_in_array = self.parse_raw_file_data(utterance[0][0])
        return utterance_in_array

    def get_new_source_data(self,connection):
        results = connection.execute(GET_NEW_SOURCE_DATA_QUERY).fetchall()
        max_date = results
        return max_date


    def update_source_metadata_table(self,connection):
        # self.insert_into_source_metadata(connection)
        # source_info = self.get_new_source_data(connection)
        # for source in source_info:
        #     update_query = text(UPDATE_SOURCE_METADATA_QUERY)
        #     connection.execute(update_query,cleaned_duration=source[0], num_audio=source[2], source_name=source[1])
        insert_query = self.update_utterance_in_mapping_table(connection)

        if len(insert_query) < 1:
            # TODO: should raise execption
            return

        default_query = DEFAULT_INSERT_QUERY

        final_query = default_query + insert_query

        connection.execute(final_query)

    def update_utterance_in_mapping_table(self,connection):
        all_data = self.fetch_unnormalized_data(connection)
        
        insert_query_into_mapping_table = []

        for onefile in all_data:
            audio_id = onefile[0]
            utterance_list = self.parse_raw_file_data(onefile[1])

            if utterance_list == None:
                print(audio_id)
                continue

            for utterance in utterance_list:
                insert_query_into_mapping_table.append(f"('{utterance['name']}',{utterance['duration']},\
                    {audio_id},{utterance['snr_value']},'{utterance['status']}','{utterance.get('reason','')}')")

        return insert_query_into_mapping_table


    def parse_raw_file_data(self, raw_file_utterance):
        try:
            data = json.loads(raw_file_utterance)
            return data
        except json.decoder.JSONDecodeError as error:
            print(error)
            pass

    def fetch_unnormalized_data(self,connection):

        fetch_query_where_speaker_is_null = text(FETCH_AND_UPDATE_QUERY_WHERE_SPEAKER_IS_NULL)
        
        return connection.execute(fetch_query_where_speaker_is_null).fetchall()

    def insert_into_source_metadata(self,connection):
        insert_query = text(INSERT_INTO_SOURCE_METADATA_QUERY)
        connection.execute(insert_query)

    def copy_data_media_speaker_mapping(self,db):
        connection = db.connect()
        max_load_date = self.get_load_date_for_mapping(connection)
        get_audio_id = text(GET_AUDIO_ID_QUERY)
        results = connection.execute(
            get_audio_id, max_load_date=max_load_date).fetchall()
        audio_ids = results
        print(len(audio_ids))
        with open("./full_query.txt", 'w') as myfile:
            myfile.write(
                f"insert into media_speaker_mapping(audio_id, speaker_id, clipped_utterance_file_name, clipped_utterance_duration,load_datetime,snr,status,fail_reason) values ")
        for audio_id in audio_ids:

            speaker_id = self.find_speaker_id(connection, audio_id)
            get_load_datetime_for_audio = self.get_load_datetime(audio_id, connection)
            utterance_list = self.get_utterance_list(connection, audio_id)
            for utterance_name_diration in utterance_list:
                self.create_insert_query(utterance_name_diration, speaker_id,
                                    audio_id, get_load_datetime_for_audio, connection)
            self.insert_file(connection,"./full_query.txt")
        print(audio_ids)


def __load_yaml_file(path):
    read_dict = {}
    with open(path, 'r') as file:
        read_dict = yaml.load(file)
    return read_dict


def create_db_engine(config_local_path):
    config_file = __load_yaml_file(config_local_path)
    db_configuration = config_file['db_configuration']
    db_name = db_configuration['db_name']
    db_user = db_configuration['db_user']
    db_pass = db_configuration['db_pass']
    cloud_sql_connection_name = db_configuration['cloud_sql_connection_name']
    db = create_engine(
        f'postgresql://{db_user}:{db_pass}@{cloud_sql_connection_name}/{db_name}')
    return db


if __name__ == "__main__":
    # Get Arguments
    job_mode = sys.argv[1]  # local,cluster
    gcs_bucket_name = sys.argv[2]  # remote_gcs bucket name
    # remote gcs path, for local it will be src/resources/local/config.yaml
    config_path = sys.argv[3]

    current_working_directory = os.getcwd()
    config_local_path = os.path.join(
        current_working_directory, "src/resources/" + job_mode + "/config.yaml")

    if (job_mode == "cluster"):
        # Download config file from GCS
        print("Downloading config file from cloud storage to local")
        obj_gcs = CloudStorageOperations()
        obj_gcs.download_to_local(bucket_name=gcs_bucket_name,
                                  source_blob_name=config_path,
                                  destination=config_local_path,
                                  is_directory=False)

    
    db = create_db_engine(config_local_path)
    connection = db.connect()

    normalizer = Db_normalizer()

    normalizer.update_source_metadata_table(connection)
    print("moving data from staging to media......")
    normalizer.copy_data_from_media_metadata_staging_to_media(db)
    print("moving data from staging to media done")
    print("moving data from staging to speaker......")
    normalizer.copy_data_from_media_metadata_staging_to_speaker(connection)
    print("moving data from staging to speaker done")
    print("moving data from staging to media_speaker_mapping ....")
    normalizer.copy_data_media_speaker_mapping(db)    
    print("moving data from staging to media_speaker_mapping done")
    
