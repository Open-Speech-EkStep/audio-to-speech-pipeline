import datetime
import json
import os

from gcs_utils import list_blobs_in_a_path, upload_blob, download_blob
from airflow.models import Variable
import pandas as pd
import ast
from sqlalchemy import create_engine
from datetime import datetime
import yaml

def get_variables():
    global bucket_name
    global integration_processed_path
    global bucket_file_list
    global db_catalog_tbl
    global report_file_name
    global report_upload_path
    global validation_report_source
    # processed_path = Variable.get("rawprocessedpath")
    # bucket_name = Variable.get("bucket")
    now = datetime.now()
    date_time = now.strftime("%m_%d_%Y_%H_%M_%S")
    bucket_name = Variable.get("bucket")
    report_upload_path = Variable.get("report_upload_path")
    integration_processed_path = Variable.get("integrationprocessedpath")
    bucket_file_list = '_bucket_file_list.csv'
    db_catalog_tbl = 'media_metadata_staging'
    validation_report_source = Variable.get("validation_report_source")
    report_file_name = f'Data_validation_report_{date_time}_{validation_report_source}.xlsx'

def get_prefix_split_length(full_path):
    full_path_split_list = full_path.split('/')
    return len(full_path_split_list), full_path_split_list


def get_file_attributes(full_path_split_list):
    file_name = full_path_split_list[-1]
    raw_file_name = file_name.split('_', 1)[1].split('.')[0]
    source = full_path_split_list[6]
    audio_id = full_path_split_list[7]
    status = full_path_split_list[8]
    return file_name, raw_file_name, source, audio_id, status


def generate_row(full_path, file_name, raw_file_name, source, audio_id, status):
    row = full_path + ',' + source + ',' + audio_id + ',' + raw_file_name + ',' + file_name + ',' + status
    return row


def generate_bucket_file_list(source):
    # get_variables()
    all_blobs = list_blobs_in_a_path(bucket_name, integration_processed_path + source)
    output_file = open(source + bucket_file_list, "w")
    output_file.write(
        'bucket_file_path' + ',' + 'source' + ',' + 'audio_id' + ',' + 'raw_file_name' + ',' + 'utterances_files_list' + ',' + 'status')
    for blob in all_blobs:
        full_path = blob.name
        if 'wav' in full_path:
            prefix_length, full_path_split_list = get_prefix_split_length(full_path)
            if prefix_length == 10:
                file_name, raw_file_name, source, audio_id, status = get_file_attributes(full_path_split_list)
                row = generate_row(full_path, file_name, raw_file_name, source, audio_id, status)
                output_file.write("\n")
                output_file.write(row)
    print("Bucket list has been generated")
    output_file.close()




def cleanse_catalog(data_catalog_raw):
    data_catalog_raw = data_catalog_raw[~data_catalog_raw.audio_id.isna()]
    data_catalog_raw.audio_id = data_catalog_raw.audio_id.astype('int')
    return data_catalog_raw


def fetch_data_catalog(source, db_catalog_tbl, db_conn_obj):
    if source == '':
        filter_string = '1=1'
    else:
        filter_string = f"source='{source}'"
    data_catalog_raw = pd.read_sql(f"SELECT * FROM {db_catalog_tbl} where {filter_string}", db_conn_obj)
    data_catalog_raw = cleanse_catalog(data_catalog_raw)
    return data_catalog_raw


def fetch_bucket_list(source, bucket_file_list):
    generate_bucket_file_list(source)
    data_bucket_raw = pd.read_csv(source + bucket_file_list)
    return data_bucket_raw


def get_bucket_list_not_in_catalog(data_catalog_exploded, data_bucket_raw):
    bucket_list_not_in_catalog = data_bucket_raw.merge(data_catalog_exploded, how='left',
                                                       on=['audio_id', 'utterances_files_list'],
                                                       suffixes=(None, "_y"))
    return bucket_list_not_in_catalog[bucket_list_not_in_catalog['raw_file_name_y'].isna()]
    # return bucket_list_not_in_catalog


def get_catalog_list_not_in_bucket(data_catalog_exploded, data_bucket_raw):
    catalog_list_not_in_bucket = data_bucket_raw.merge(data_catalog_exploded, how='right',
                                                       on=['audio_id', 'utterances_files_list'],
                                                       suffixes=("_x", None))
    return catalog_list_not_in_bucket[catalog_list_not_in_bucket['raw_file_name_x'].isna()]
    # return catalog_list_not_in_bucket


def explode_utterances(data_catalog_raw):
    data_catalog_raw['utterances_files_list'].fillna('[]', inplace=True)
    data_catalog_raw.utterances_files_list = data_catalog_raw.utterances_files_list.apply(ast.literal_eval)
    data_catalog_exploded = data_catalog_raw.explode('utterances_files_list').reset_index(drop=True)
    utterances_file_duration = data_catalog_exploded.utterances_files_list.astype('str').apply(
        lambda x: float(x.split(':')[-1]))
    data_catalog_exploded.insert(5, 'utterances_file_duration', utterances_file_duration)
    data_catalog_exploded.utterances_files_list = data_catalog_exploded.utterances_files_list.astype('str').apply(
        lambda x: x.split(':')[0])
    return data_catalog_exploded


def get_bucket_list_in_catalog(data_catalog_exploded, data_bucket_raw):
    bucket_list_in_catalog = data_bucket_raw.merge(data_catalog_exploded, how='inner',
                                                   on=['audio_id', 'utterances_files_list'],
                                                   suffixes=("_x", None))
    return bucket_list_in_catalog


def get_invalid_utterance_duration(bucket_list_in_catalog):
    return bucket_list_in_catalog[~bucket_list_in_catalog.utterances_file_duration.between(.5, 15)].sort_values(
        'raw_file_name')


def append_file_and_duration(valid_utterance_duration):
    valid_utterance_duration['utterances_files_list'] = valid_utterance_duration[
        ['utterances_files_list', 'utterances_file_duration']].apply(lambda x: x[0] + ':' + str(x[1]), axis=1)
    return valid_utterance_duration


def get_valid_utterance_duration_unexploded(bucket_list_in_catalog):
    valid_utterance_duration = bucket_list_in_catalog[
        bucket_list_in_catalog.utterances_file_duration.between(.5, 15)].sort_values(
        'raw_file_name')
    valid_utterance_duration = append_file_and_duration(valid_utterance_duration)

    return valid_utterance_duration.groupby('audio_id', as_index=False).agg(
        {'utterances_file_duration': lambda x: x.sum() / 60,
         'utterances_files_list': lambda tdf: tdf.tolist()})


def get_duplicates_utterances(data_catalog_raw):
    return data_catalog_raw[
        data_catalog_raw.duplicated(subset=['raw_file_name'], keep=False)].sort_values(
        by='raw_file_name')


def get_unique_utterances(data_catalog_raw):
    return data_catalog_raw[~data_catalog_raw.duplicated(subset=['raw_file_name'])]


def get_valid_and_unique_utterances(df_catalog_unique, df_catalog_valid_utterance_duration_unexploded):
    df_valid_utterances_with_unique_audioid = df_catalog_valid_utterance_duration_unexploded.merge(df_catalog_unique,
                                                                                                   on='audio_id',
                                                                                                   suffixes=('', '_y'))
    df_valid_utterances_with_unique_audioid['cleaned_duration'] = df_valid_utterances_with_unique_audioid[
        'utterances_file_duration']

    df_valid_utterances_with_unique_audioid.drop(['utterances_files_list_y', 'utterances_file_duration'], axis=1,
                                                 inplace=True)
    return df_valid_utterances_with_unique_audioid


# def get_bucket_list_in_catalog_unexploded(bucket_list_in_catalog, data_catalog_raw):
#     audio_ids_in_bucket_and_catalog = bucket_list_in_catalog[~bucket_list_in_catalog.duplicated(subset=['audio_id'])][[
#         'audio_id']]
#     return data_catalog_raw.merge(audio_ids_in_bucket_and_catalog,
#                                   on='audio_id')
#

def generate_data_validation_report(data_catalog_raw, data_bucket_raw):
    print("Generate reports...")
    # get_variables()
    data_catalog_exploded = explode_utterances(data_catalog_raw)
    bucket_list_not_in_catalog = get_bucket_list_not_in_catalog(data_catalog_exploded, data_bucket_raw)
    catalog_list_not_in_bucket = get_catalog_list_not_in_bucket(data_catalog_exploded, data_bucket_raw)
    bucket_list_in_catalog = get_bucket_list_in_catalog(data_catalog_exploded, data_bucket_raw)
    bucket_list_in_catalog_cleaned = bucket_list_in_catalog[bucket_list_in_catalog.status == 'clean']
    catalog_list_with_rejected_status = bucket_list_in_catalog[bucket_list_in_catalog.status == 'rejected']
    df_catalog_invalid_utterance_duration = get_invalid_utterance_duration(bucket_list_in_catalog)
    df_catalog_valid_utterance_duration_unexploded = get_valid_utterance_duration_unexploded(
        bucket_list_in_catalog_cleaned)
    df_catalog_duplicates = get_duplicates_utterances(data_catalog_raw)
    df_catalog_unique = get_unique_utterances(data_catalog_raw)

    df_valid_utterances_with_unique_audioid = get_valid_and_unique_utterances(df_catalog_unique,
                                                                              df_catalog_valid_utterance_duration_unexploded)
    writer = pd.ExcelWriter(report_file_name, engine='xlsxwriter')
    bucket_list_not_in_catalog.to_excel(writer, sheet_name='bucket_list_not_in_catalog', index=False)
    catalog_list_not_in_bucket.to_excel(writer, sheet_name='catalog_list_not_in_bucket', index=False)
    bucket_list_in_catalog.to_excel(writer, sheet_name='catalog_list_in_bucket', index=False)
    catalog_list_with_rejected_status.to_excel(writer, sheet_name='catalog_list_rejected_ones', index=False)
    df_catalog_invalid_utterance_duration.to_excel(writer, sheet_name='catalog_list_invalid_duration', index=False)
    df_catalog_duplicates.to_excel(writer, sheet_name='catalog_list_with_duplicates', index=False)
    df_valid_utterances_with_unique_audioid.to_excel(writer, sheet_name='Cleaned_data_catalog', index=False)
    writer.save()
    print(f"{report_file_name} has been generated....")


# generate_bucket_file_list(source)
def fetch_data(source,db_conn_obj):
    # get_variables()
    print("Pulling data from bucket and catalog...")
    data_catalog_raw = fetch_data_catalog(source, db_catalog_tbl, db_conn_obj)
    data_bucket_raw = fetch_bucket_list(source, bucket_file_list)
    return data_catalog_raw, data_bucket_raw


def upload_report_to_bucket():
    # get_variables()
    print("Uploading report to bucket ...")
    upload_blob(bucket_name, report_file_name, report_upload_path + report_file_name)
    os.remove(report_file_name)


def __load_yaml_file(path):
    read_dict = {}
    with open(path, 'r') as file:
        read_dict = yaml.safe_load(file)
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


def get_db_connection_object():
    config_path = "./config.yaml"
    download_blob(bucket_name, "data/audiotospeech/config/downloaded_data_cataloguer/config.yaml",
                  config_path)
    return create_db_engine(config_path)


def report_generation_pipeline():
    get_variables()
    source = validation_report_source
    data_catalog_raw, data_bucket_raw = fetch_data(source,get_db_connection_object())
    generate_data_validation_report(data_catalog_raw, data_bucket_raw)
    upload_report_to_bucket()
