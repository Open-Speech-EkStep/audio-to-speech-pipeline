import datetime
import json
import os

from gcs_utils import list_blobs_in_a_path, upload_blob, download_blob
import pandas as pd
import ast
from sqlalchemy import create_engine
from datetime import datetime
import yaml
import numpy as np


def get_config_variables():
    config_path = "./config.yaml"
    download_blob("ekstepspeechrecognition-dev", "data/audiotospeech/config/validation_report_dag/config.yaml",
                  config_path)
    variables = __load_yaml_file(config_path)["report_configuration"]
    return variables


def get_local_variables():
    global validation_report_source
    get_common_variables()
    validation_report_source = ["CEC"]


def get_common_variables():
    global bucket_name
    global integration_processed_path
    global bucket_file_list
    global db_catalog_tbl
    global report_upload_path
    variables = get_config_variables()
    bucket_name = variables["bucket_name"]
    report_upload_path = variables["report_upload_path"]
    integration_processed_path = variables["integration_processed_path"]
    db_catalog_tbl = variables["db_catalog_tbl"]
    bucket_file_list = '_bucket_file_list.csv'


def get_variables():
    from airflow.models import Variable
    global validation_report_source
    get_common_variables()
    validation_report_source = Variable.get("validation_report_source")


def set_report_names(source):
    global report_file_name
    global cleaned_csv_report_file_name
    now = datetime.now()
    date_time = now.strftime("%m_%d_%Y_%H_%M_%S")
    report_file_name = f'Data_validation_report_{date_time}_{source}.xlsx'
    cleaned_csv_report_file_name = f"Final_Report_{date_time}_{source}.csv"


def get_prefix_attributes_full_path(full_path, integration_processed_path):
    source_prefix_split_list = full_path.split(integration_processed_path)[1].split('/')
    return get_file_attributes(source_prefix_split_list)


def get_file_attributes(source_prefix_split_list):
    file_name = source_prefix_split_list[-1]
    try:
        raw_file_name = file_name.split('_', 1)[1].split('.')[0]
    except:
        raw_file_name = "NA"
    source = source_prefix_split_list[0]
    audio_id = source_prefix_split_list[1]
    status = source_prefix_split_list[2]
    return file_name, raw_file_name, source, audio_id, status


def generate_row(full_path, file_name, raw_file_name, source, audio_id, status):
    row = full_path + ',' + source + ',' + audio_id + ',' + raw_file_name + ',' + file_name + ',' + status
    return row


def generate_bucket_file_list(source):
    # get_variables()
    all_blobs = list_blobs_in_a_path(bucket_name, integration_processed_path + source)
    output_file = open(source + bucket_file_list, "w")
    output_file.write(
        'bucket_file_path' + ',' + 'source' + ',' + 'audio_id' + ',' + 'raw_file_name' + ',' + 'utterances_file_name' + ',' + 'status')
    for blob in all_blobs:
        full_path = str(blob.name).replace(",", "")
        try:
            if 'wav' in full_path:
                file_name, raw_file_name, source, audio_id, status = get_prefix_attributes_full_path(full_path,
                                                                                                     integration_processed_path)
                row = generate_row(full_path, file_name, raw_file_name, source, audio_id, status)
                output_file.write("\n")
                output_file.write(row)
        except:
            print(f"Failed at {full_path}")
    print("Bucket list has been generated")
    output_file.close()


def cleanse_catalog(data_catalog_raw):
    data_catalog_raw = data_catalog_raw[~data_catalog_raw.audio_id.isna()]
    return data_catalog_raw


def fetch_data_catalog(source, db_catalog_tbl, db_conn_obj):
    if source == '':
        filter_string = '1=1'
    else:
        filter_string = f"source='{source}'"
    data_catalog_raw = pd.read_sql(f"SELECT * FROM {db_catalog_tbl} where {filter_string}", db_conn_obj)
    data_catalog_raw = cleanse_catalog(data_catalog_raw)
    data_catalog_raw["raw_file_name"] = data_catalog_raw.raw_file_name.apply(lambda x: x.replace(",", ""))
    return data_catalog_raw


def fetch_bucket_list(source, bucket_file_list):
    generate_bucket_file_list(source)
    data_bucket_raw = pd.read_csv(source + bucket_file_list, low_memory=False)
    os.remove(source + bucket_file_list)
    return data_bucket_raw


def get_bucket_list_not_in_catalog(data_catalog_exploded, data_bucket_raw):
    bucket_list_not_in_catalog = data_bucket_raw.merge(data_catalog_exploded, how='left',
                                                       on=['audio_id', 'utterances_file_name'],
                                                       suffixes=(None, "_y"))
    return bucket_list_not_in_catalog[bucket_list_not_in_catalog['raw_file_name_y'].isna()]
    # return bucket_list_not_in_catalog


def get_catalog_list_not_in_bucket(data_catalog_exploded, data_bucket_raw):
    catalog_list_not_in_bucket = data_bucket_raw.merge(data_catalog_exploded, how='right',
                                                       on=['audio_id', 'utterances_file_name'],
                                                       suffixes=("_x", None))
    return catalog_list_not_in_bucket[catalog_list_not_in_bucket['raw_file_name_x'].isna()]
    # return catalog_list_not_in_bucket


def parse_json_utterance_meta(jsonData):
    json_dict = json.loads(jsonData)
    return json_dict['name'] + ',' + str(json_dict['duration']) + ',' + json_dict['status']


def check_json_utterance_meta(jsonData):
    print(f"Utterance_file_list being processed is {jsonData}")
    # try:
    #     json.loads(jsonData)
    # except (ValueError, TypeError):
    #     return False
    # return True
    if type(jsonData) == dict:
        return True
    else:
        return False


def parse_string_utterance_meta(data):
    data = data.replace(",", "")
    filename = data.split(':')[0]
    duration = data.split(':')[-1]
    status = ''
    return filename + ',' + str(duration) + ',' + status


def convert_string_utterance_meta(data):
    dicti = {}
    data = data.replace(",", "")
    filename = data.split(':')[0]
    duration = data.split(':')[-1]
    status = np.nan
    snr = np.nan
    reason = np.nan
    dicti["name"] = filename
    dicti["duration"] = duration
    dicti["snr_value"] = snr
    dicti["status"] = status
    dicti["reason"] = reason
    return json.dumps(dicti)


def explode_utterances(data_catalog_raw):
    data_catalog_raw['utterances_files_list'].fillna('[]', inplace=True)
    data_catalog_raw.utterances_files_list = data_catalog_raw.utterances_files_list.astype('str').apply(
        ast.literal_eval)
    data_catalog_exploded = data_catalog_raw.explode('utterances_files_list').reset_index(drop=True)
    return data_catalog_exploded


def normalize_exploded_utterances(data_catalog_raw):
    data_catalog_exploded = explode_utterances(data_catalog_raw)
    utterances_files_list_meta = data_catalog_exploded.utterances_files_list.apply(
        lambda x: parse_json_utterance_meta(json.dumps(x)) if check_json_utterance_meta(
            x) else parse_string_utterance_meta(str(x)))
    utterances_files_list_meta = utterances_files_list_meta.str.split(
        ",", expand=True)
    data_catalog_exploded["utterances_files_list"] = data_catalog_exploded.utterances_files_list.apply(
        lambda x: json.dumps(x) if check_json_utterance_meta(
            x) else convert_string_utterance_meta(str(x)))
    data_catalog_exploded.insert(5, 'utterances_file_name', utterances_files_list_meta[0])
    data_catalog_exploded.insert(6, 'utterances_file_duration', utterances_files_list_meta[1].astype('float'))
    data_catalog_exploded.insert(7, 'utterances_file_status', utterances_files_list_meta[2])
    return data_catalog_exploded


def get_bucket_list_in_catalog(data_catalog_exploded, data_bucket_raw):
    bucket_list_in_catalog = data_bucket_raw.merge(data_catalog_exploded, how='inner',
                                                   on=['audio_id', 'utterances_file_name'],
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
    # valid_utterance_duration = append_file_and_duration(valid_utterance_duration)
    valid_utterance_duration = valid_utterance_duration[valid_utterance_duration.status.str.lower() != "rejected"]
    valid_utterance_duration = valid_utterance_duration[
        valid_utterance_duration.utterances_file_status.str.lower() != "rejected"]
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

def append_transcription_files_list(df_cleaned_dataset):
    df_cleaned_dataset_transcipts = df_cleaned_dataset.copy()
    df_cleaned_dataset_transcipts.bucket_file_path = df_cleaned_dataset_transcipts.bucket_file_path.apply(
        lambda x: x.replace('wav', 'txt'))
    df_cleaned_dataset_transcipts.utterances_file_name = df_cleaned_dataset_transcipts.utterances_file_name.apply(
        lambda x: x.replace('wav', 'txt'))
    df_cleaned_dataset.insert(loc=1, column="transcriptions_path_bucket",
                              value=df_cleaned_dataset_transcipts["bucket_file_path"].values.tolist())
    df_cleaned_dataset = df_cleaned_dataset.drop(["utterances_file_name"], axis=1)
    df_cleaned_dataset = df_cleaned_dataset.rename(
        columns={"bucket_file_path": "wav_path_bucket", "utterances_file_duration": "duration"})
    df_cleaned_dataset.wav_path_bucket = df_cleaned_dataset.wav_path_bucket.apply(
        lambda x: "gs://" + bucket_name + "/" + x)
    df_cleaned_dataset.transcriptions_path_bucket = df_cleaned_dataset.transcriptions_path_bucket.apply(
        lambda x: "gs://" + bucket_name + "/" + x)
    return df_cleaned_dataset


def generate_cleaned_dataset(df_cleaned_utterances_unexploded, bucket_list_in_catalog):
    df_cleaned_dataset = df_cleaned_utterances_unexploded.merge(bucket_list_in_catalog,
                                                                on=['audio_id', 'utterances_files_list'],
                                                                suffixes=('_y', ' '))
    return append_transcription_files_list(
        df_cleaned_dataset[['bucket_file_path', 'utterances_file_name', 'utterances_file_duration']])


def generate_data_validation_report(data_catalog_raw, data_bucket_raw):
    print("Generate reports...")
    data_catalog_exploded = normalize_exploded_utterances(data_catalog_raw)
    print(data_catalog_exploded.utterances_files_list)
    bucket_list_not_in_catalog = get_bucket_list_not_in_catalog(data_catalog_exploded, data_bucket_raw)
    catalog_list_not_in_bucket = get_catalog_list_not_in_bucket(data_catalog_exploded, data_bucket_raw)
    bucket_list_in_catalog = get_bucket_list_in_catalog(data_catalog_exploded, data_bucket_raw)
    # bucket_list_in_catalog_cleaned = bucket_list_in_catalog[bucket_list_in_catalog.status == 'clean']
    # catalog_list_with_rejected_status = bucket_list_in_catalog[bucket_list_in_catalog.status == 'rejected']
    df_catalog_invalid_utterance_duration = get_invalid_utterance_duration(bucket_list_in_catalog)
    df_catalog_valid_utterance_duration_unexploded = get_valid_utterance_duration_unexploded(
        bucket_list_in_catalog)
    df_catalog_duplicates = get_duplicates_utterances(data_catalog_raw)
    df_catalog_unique = get_unique_utterances(data_catalog_raw)

    df_valid_utterances_with_unique_audioid = get_valid_and_unique_utterances(df_catalog_unique,
                                                                              df_catalog_valid_utterance_duration_unexploded)

    df_cleaned_utterances_unexploded = explode_utterances(df_valid_utterances_with_unique_audioid
                                                          )
    df_cleaned_dataset = generate_cleaned_dataset(df_cleaned_utterances_unexploded, bucket_list_in_catalog)

    writer = pd.ExcelWriter(report_file_name, engine='xlsxwriter')
    bucket_list_not_in_catalog.to_excel(writer, sheet_name='bucket_list_not_in_catalog', index=False)
    catalog_list_not_in_bucket.to_excel(writer, sheet_name='catalog_list_not_in_bucket', index=False)
    bucket_list_in_catalog.to_excel(writer, sheet_name='catalog_list_in_bucket', index=False)
    # catalog_list_with_rejected_status.to_excel(writer, sheet_name='catalog_list_rejected_ones', index=False)
    df_catalog_invalid_utterance_duration.to_excel(writer, sheet_name='catalog_list_invalid_duration', index=False)
    df_catalog_duplicates.to_excel(writer, sheet_name='catalog_list_with_duplicates', index=False)
    df_valid_utterances_with_unique_audioid.to_excel(writer, sheet_name='Cleaned_data_catalog', index=False)
    writer.save()
    df_cleaned_dataset.to_csv(cleaned_csv_report_file_name, index=False)
    print(f"{report_file_name} has been generated....")
    print(f"{cleaned_csv_report_file_name} has been generated....")


# generate_bucket_file_list(source)
def fetch_data(source, db_conn_obj):
    # get_variables()
    print("Pulling data from bucket and catalog...")
    data_catalog_raw = fetch_data_catalog(source, db_catalog_tbl, db_conn_obj)
    data_bucket_raw = fetch_bucket_list(source, bucket_file_list)
    return data_catalog_raw, data_bucket_raw


def upload_report_to_bucket():
    # get_variables()
    print("Uploading report to bucket ...")
    upload_blob(bucket_name, report_file_name, os.path.join(report_upload_path, report_file_name))
    upload_blob(bucket_name, cleaned_csv_report_file_name,
                os.path.join(report_upload_path, "Final_csv_reports", cleaned_csv_report_file_name))
    os.remove(report_file_name)
    os.remove(cleaned_csv_report_file_name)


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
    return create_db_engine(config_path)


def check_dataframes(data_catalog_raw, data_bucket_raw):
    catalouge_len = len(data_catalog_raw)
    bucket_len = len(data_bucket_raw)
    if (catalouge_len == 0 and bucket_len != 0):
        print("For the given source no data in catalouge")
        exit(1)
    elif (catalouge_len != 0 and bucket_len == 0):
        print("For the given source no data in bucket")
        exit(1)
    elif (catalouge_len == 0 and bucket_len == 0):
        print("For the given source no data found. Check the path and source name!!!!")
        exit(1)
    else:
        pass


def report_generation_pipeline(mode="cluster"):
    if mode == "local":
        get_local_variables()
    else:
        get_variables()
    source_list = ast.literal_eval(str(validation_report_source))
    if len(source_list) == 0:
        source_list.append("")
    for _source in source_list:
        set_report_names(_source)
        data_catalog_raw, data_bucket_raw = fetch_data(_source, get_db_connection_object())
        check_dataframes(data_catalog_raw, data_bucket_raw)
        generate_data_validation_report(data_catalog_raw, data_bucket_raw)
        upload_report_to_bucket()


if __name__ == "__main__":
    report_generation_pipeline("local")
