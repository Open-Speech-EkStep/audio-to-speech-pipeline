import os
import json
import csv
import yaml
import sys
from .db_query import GET_NEW_SPEAKER, GET_EXPERIMENT_ID, GET_ALL_SPEAKER_ID_FROM_GIVEN_EXP, INSERT_NEW_EXPERIMENT, INITIAL_TEXT_OF_INSERT_QUERY, GET_NEW_SPEAKER, GET_UTTERANCES_OF_GIVEN_EXP, GET_UTTERANCES_OF_NEW_USER, INITIAL_TEXT_OF_UPDATE_QUERY, GET_ALL_DATA_OF_CURRENT_EXP
from os.path import join, dirname
from sqlalchemy import create_engine, select, MetaData, Table, text
from sqlalchemy.orm import Session
from .gcs_operations import CloudStorageOperations


class ExperimentDataTagger():

    def get_existing_experiment_id(self, connection):
        query = text(GET_EXPERIMENT_ID)
        exp_id = connection.execute(
            query, exp_name=existing_experiment_name).fetchall()
        return exp_id[0][0]

    # def create_copy_table():
    #     connection = db.connect()
    #     connection.execute(
    #         "CREATE TABLE  media_speaker_mapping_test1 AS TABLE media_speaker_mapping_backup_new_column;")

    def insert_and_get_exp_id(self, connection):
        query = text(INSERT_NEW_EXPERIMENT)
        get_experiment_id = connection.execute(
            query, name=experiment_name, desc=experiment_description).fetchall()
        current_exp_id = get_experiment_id[0][0]
        print("insert new experiment")
        return current_exp_id

    def create_new_experiment(self, db):
        connection = db.connect()
        trans = connection.begin()
        try:
            current_exp_id = self.insert_and_get_exp_id(connection)
            if use_existing_experiment_data:
                self.for_new_experiment_with_using_existing_exp(
                    connection, current_exp_id, trans)
            self.create_qurey_and_update_table_for_new_speaker(
                connection, current_exp_id)
            trans.commit()
            # get_all_tegged_data_csv(current_exp_id)
            return current_exp_id
        except:
            trans.rollback()
            raise

    def for_new_experiment_with_using_existing_exp(self, connection, current_exp_id, trans):
        existing_experiment_id = self.get_existing_experiment_id(connection)
        query = text(GET_ALL_SPEAKER_ID_FROM_GIVEN_EXP)
        get_all_speaker_id = connection.execute(
            query, time=duration_per_speaker_in_second, id=existing_experiment_id, require_speaker_from_existing_exp=number_of_speaker_from_existing_experiment).fetchall()
        for speaker_id in get_all_speaker_id:
            get_all_utterances = self.get_utterances_for_existing_experiment_data(
                connection, speaker_id[0], existing_experiment_id)
            insert_query = self.create_insert_query(
                get_all_utterances, speaker_id[0], current_exp_id)
            self.insert_into_media_speaker_mapping(connection, insert_query)

    def insert_into_media_speaker_mapping(self, connection, insert_query):
        clear_content = insert_query[:-1]
        # connection = db.connect()
        connection.execute(clear_content)
        # trans.commit()
        print("insertion done")

    def create_insert_query(self, get_all_utterances, speaker_id, current_exp_id, clean_duration=0):
        insert_query = INITIAL_TEXT_OF_INSERT_QUERY
        for utterance in get_all_utterances:
            clean_duration += utterance[2]
            if clean_duration >= duration_per_speaker_in_second:
                return insert_query
        insert_query = insert_query + \
            f"({utterance[6]},{speaker_id},'{utterance[1]}',{utterance[2]},'{utterance[4]}',{current_exp_id},true,true),"

    def find_all_speaker(self, connection):
        query = text(GET_NEW_SPEAKER)
        results = connection.execute(
            query, time=duration_per_speaker_in_second, require_speaker=require_new_speaker).fetchall()
        return results

    def get_utterances_for_existing_experiment_data(self, connection, speaker_id, exp_id):
        query = text(GET_UTTERANCES_OF_GIVEN_EXP)
        results = connection.execute(
            query, speaker_id=speaker_id, exp_id=exp_id).fetchall()
        return results

    def get_utterances(self, connection, speaker_id):
        query = text(GET_UTTERANCES_OF_NEW_USER)
        results = connection.execute(
            query, speaker_id=speaker_id).fetchall()
        return results

    def clean_duration_threshold(self, get_all_utterances, current_exp_id, clean_duration=0):
        update_utterances_query = f"update media_speaker_mapping_test_with_yaml set experiment_use_status = true,experiment_id= {current_exp_id} WHERE clipped_utterance_file_name IN ( "
        for utterance in get_all_utterances:
            clean_duration += utterance[2]
            if clean_duration >= duration_per_speaker_in_second:
                return update_utterances_query
            update_utterances_query = update_utterances_query + \
                f"'{utterance[1]}',"
        return update_utterances_query

    def update_table(self, connection, query):
        remove_last_comma = query[:-1]
        clear_content = remove_last_comma + ")"
        # connection = db.connect()
        print(clear_content)
        connection.execute(clear_content)
        print("update done")

    def create_qurey_and_update_table_for_new_speaker(self, connection, current_exp_id):
        get_all_speaker_id = self.find_all_speaker(connection)
        update_query_for_all_speaker = INITIAL_TEXT_OF_UPDATE_QUERY
        for speaker_id in get_all_speaker_id:
            update_query_for_all_speaker = update_query_for_all_speaker + \
                f"{speaker_id[0]},"
            get_all_utterances = self.get_utterances(connection, speaker_id[0])
            update_query = self.clean_duration_threshold(
                get_all_utterances, current_exp_id)
            self.update_table(connection, update_query)
        self.update_table(connection, update_query_for_all_speaker)


def get_all_tegged_data_csv(exp_id):
    connection = db.connect()
    query = text(GET_ALL_DATA_OF_CURRENT_EXP)
    result = connection.execute(query, exp_id=exp_id).fetchall()
    with open(metadata_output_path+'/all_tagged_data.csv', 'w') as f:
        out = csv.writer(f)
        out.writerow(['audio_id', 'clipped_utterance_file_name',
                      'source', 'experiment_name'])
        for index, item in enumerate(result):
            if index % 5000 == 0 and index != 0:
                f.close()
                f = open(
                    f'{metadata_output_path}/all_tagged_data{index}.csv', 'w')
                out = csv.writer(f)
                out.writerow(
                    ['audio_id', 'clipped_utterance_file_name', 'source', 'experiment_name'])
            out.writerow(item)


def get_variables(config_file_path):
    config_file = __load_yaml_file(config_file_path)
    configuration = config_file['configuration']
    global experiment_name
    global num_speakers
    global experiment_description
    global duration_per_speaker_in_second
    global use_existing_experiment_data
    global require_new_speaker
    global existing_experiment_name
    global number_of_speaker_from_existing_experiment
    global metadata_output_path

    experiment_name = configuration['experiment_name']
    num_speakers = configuration['num_speakers']
    experiment_description = configuration['experiment_description']
    duration_per_speaker_in_minute = configuration['duration_per_speaker']
    use_existing_experiment_data = configuration['use_existing_experiment_data']
    existing_experiment_name = configuration['existing_experiment_name']
    metadata_output_path = configuration['metadata_output_path']
    number_of_speaker_from_existing_experiment = configuration[
        'number_of_speaker_from_existing_experiment']
    duration_per_speaker_in_second = duration_per_speaker_in_minute*60
    require_new_speaker = num_speakers-number_of_speaker_from_existing_experiment


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

    obj_gcs = CloudStorageOperations()
    if (job_mode == "cluster"):
        # Download config file from GCS
        print("Downloading config file from cloud storage to local")
        obj_gcs.download_to_local(bucket_name=gcs_bucket_name,
                                  source_blob_name=config_path,
                                  destination=config_local_path,
                                  is_directory=False)

    get_variables(config_local_path)

    db = create_db_engine(config_local_path)

    tagging_data = ExperimentDataTagger()
    print("tagging data started.......")
    current_exp_id = tagging_data.create_new_experiment(db)
    print("tagging is done")
    print("genration of csv is started")
    obj_gcs.make_directories(os.path.join(current_working_directory,metadata_output_path))
    get_all_tegged_data_csv(current_exp_id)
    print("genration csv is done")
    print("uploading csv to gcs...")
    if (job_mode == "cluster"):
        obj_gcs.upload_to_gcs(bucket_name=gcs_bucket_name,
                                    source=os.path.join(current_working_directory,metadata_output_path),
                                    destination_blob_name=metadata_output_path,
                                    is_directory=True)
