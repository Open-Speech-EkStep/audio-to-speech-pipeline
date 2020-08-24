import datetime
import json
import os

from gcs_utils import list_blobs_in_a_path, copy_blob, check_blob, \
    move_blob, upload_blob, read_blob, move_directory
# from airflow.models import Variable
import pandas as pd
import numpy as np
import seaborn as sns
import ast
from sqlalchemy import create_engine
from datetime import datetime


def get_variables():
    global bucket_name
    global processed_path
    global bucket_file_list
    global db_conn_string
    global db_catalog_tbl
    # processed_path = Variable.get("rawprocessedpath")
    # bucket_name = Variable.get("bucket")
    bucket_name = 'ekstepspeechrecognition-dev'
    processed_path = 'data/audiotospeech/integration/processed/hindi/audio/'
    bucket_file_list = '_bucket_file_list.csv'
    db_conn_string = ''
    db_catalog_tbl = 'media_metadata_staging'


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
    get_variables()
    all_blobs = list_blobs_in_a_path(bucket_name, processed_path + source)
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


def get_db_connection(db_conn_string):
    alchemy_engine = create_engine(db_conn_string)
    print("Connection created")
    return alchemy_engine.connect()


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


def get_valid_utterance_duration_unexploded(bucket_list_in_catalog):
    valid_utterance_duration = bucket_list_in_catalog[
        bucket_list_in_catalog.utterances_file_duration.between(.5, 15)].sort_values(
        'raw_file_name')
    return valid_utterance_duration.groupby('audio_id', as_index=False).agg(
        {'utterances_file_duration': lambda x: x.sum() / 60,
         'utterances_files_list': lambda tdf: tdf.tolist()})


def get_duplicates_utterances(bucket_list_in_catalog_unexploded):
    return bucket_list_in_catalog_unexploded[
        bucket_list_in_catalog_unexploded.duplicated(subset=['raw_file_name'], keep=False)].sort_values(
        by='raw_file_name')


def get_unique_utterances(bucket_list_in_catalog_unexploded):
    return bucket_list_in_catalog_unexploded[~bucket_list_in_catalog_unexploded.duplicated(subset=['raw_file_name'])]


def get_valid_and_unique_utterances(df_catalog_unique, df_catalog_valid_utterance_duration_unexploded):
    df_valid_utterances_with_unique_audioid = df_catalog_unique.merge(df_catalog_valid_utterance_duration_unexploded,
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

def generate_data_validation_report(source, data_catalog_raw, data_bucket_raw):
    data_catalog_exploded = explode_utterances(data_catalog_raw)
    # print(data_catalog_exploded.head())
    bucket_list_not_in_catalog = get_bucket_list_not_in_catalog(data_catalog_exploded, data_bucket_raw)
    catalog_list_not_in_bucket = get_catalog_list_not_in_bucket(data_catalog_exploded, data_bucket_raw)
    bucket_list_in_catalog = get_bucket_list_in_catalog(data_catalog_exploded, data_bucket_raw)
    catalog_list_with_rejected_status = bucket_list_in_catalog[bucket_list_in_catalog.status == 'rejected']

    df_catalog_invalid_utterance_duration = get_invalid_utterance_duration(bucket_list_in_catalog)
    df_catalog_valid_utterance_duration_unexploded = get_valid_utterance_duration_unexploded(bucket_list_in_catalog)
    df_catalog_duplicates = get_duplicates_utterances(data_catalog_raw)
    df_catalog_unique = get_unique_utterances(data_catalog_raw)

    df_valid_utterances_with_unique_audioid = get_valid_and_unique_utterances(df_catalog_unique,
                                                                              df_catalog_valid_utterance_duration_unexploded)

    now = datetime.now()
    date_time = now.strftime("%m_%d_%Y_%H_%M_%S")
    writer = pd.ExcelWriter(f'Data_validation_report_{source}_{date_time}.xlsx', engine='xlsxwriter')
    bucket_list_not_in_catalog.to_excel(writer, sheet_name='bucket_list_not_in_catalog', index=False)
    catalog_list_not_in_bucket.to_excel(writer, sheet_name='catalog_list_not_in_bucket', index=False)
    catalog_list_with_rejected_status.to_excel(writer, sheet_name='catalog_list_rejected_ones', index=False)
    df_catalog_invalid_utterance_duration.to_excel(writer, sheet_name='catalog_list_invalid_duration', index=False)
    df_catalog_duplicates.to_excel(writer, sheet_name='catalog_list_with_duplicates', index=False)
    df_valid_utterances_with_unique_audioid.to_excel(writer, sheet_name='Cleaned_data_catalog', index=False)
    writer.save()





# generate_bucket_file_list(source)
def fetch_data(source):
    get_variables()
    db_conn_obj = get_db_connection(db_conn_string)
    data_catalog_raw = fetch_data_catalog(source, db_catalog_tbl, db_conn_obj)
    data_bucket_raw = fetch_bucket_list(source, bucket_file_list)
    return data_catalog_raw, data_bucket_raw


if __name__ == "__main__":
    source = 'joshtalks'
    data_catalog_raw, data_bucket_raw = fetch_data(source)
    generate_data_validation_report(source, data_catalog_raw, data_bucket_raw)
