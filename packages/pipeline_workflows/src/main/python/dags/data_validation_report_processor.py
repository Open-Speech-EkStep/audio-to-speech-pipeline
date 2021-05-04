import ast
import datetime
import json
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import yaml
from sqlalchemy import create_engine

from gcs_utils import list_blobs_in_a_path, upload_blob, download_blob

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", -1)
LANGUAGE_CONSTANT = "{language}"
UTTERANCE_FILE_FORMAT = "wav"
TRANSCRIPT_FILE_FORMAT = "txt"


def get_config_variables(stage):
    global config_path
    config_path = f"./config_{stage}.yaml"
    print("Downloading config file for validation report job")
    download_blob(
        bucket_name,
        f"data/audiotospeech/config/validation_report_dag/config_{stage}.yaml",
        config_path,)

    variables = __load_yaml_file()["report_configuration"]
    return variables


def get_local_variables(bucket, sources):
    global validation_report_sources
    global bucket_name
    bucket_name = bucket
    # get_common_variables(stage, language)
    validation_report_sources = sources


def get_common_variables(stage, language):
    global bucket_name
    global integration_processed_path
    global bucket_file_list
    global db_catalog_tbl
    global db_mapping_tbl
    global report_upload_path
    variables = get_config_variables(stage)
    report_upload_path = variables["report_upload_path"].replace(
        LANGUAGE_CONSTANT, language
    )
    integration_processed_path = variables["integration_processed_path"].replace(
        LANGUAGE_CONSTANT, language
    )
    db_catalog_tbl = variables["db_catalog_tbl"]
    db_mapping_tbl = variables["db_mapping_tbl"]
    bucket_file_list = "_bucket_file_list.csv"
    print(f"report_upload_path is {report_upload_path}")
    print(f"integration_processed_path is {integration_processed_path}")


def get_airflow_variables(stage):
    from airflow.models import Variable

    global validation_report_sources
    global bucket_name
    bucket_name = Variable.get("bucket")
    # language = Variable.get("language").lower()
    # get_common_variables(stage, language)
    validation_report_sources = json.loads(Variable.get("validation_report_source_" + stage))


def set_report_names(source):
    global report_file_name
    global cleaned_csv_report_file_name
    now = datetime.now()
    date_time = now.strftime("%m_%d_%Y_%H_%M_%S")
    report_file_name = f"Data_validation_report_{date_time}_{source}.xlsx"
    cleaned_csv_report_file_name = f"Final_Report_{date_time}_{source}.csv"


def get_prefix_attributes_full_path(full_path, integration_processed_path):
    source_prefix_split_list = full_path.split(integration_processed_path + "/")[
        1
    ].split("/")
    return get_file_attributes(source_prefix_split_list)


def get_file_attributes(source_prefix_split_list):
    file_name = source_prefix_split_list[-1]
    try:
        raw_file_name = file_name.split("_", 1)[1].split(".")[0]
    except BaseException:
        raw_file_name = "NA"
    source = source_prefix_split_list[0]
    audio_id = source_prefix_split_list[1]
    status = source_prefix_split_list[2]
    return file_name, raw_file_name, source, audio_id, status


def generate_row(full_path, file_name, raw_file_name, source, audio_id, status):
    row = (
            full_path
            + ","
            + source
            + ","
            + audio_id
            + ","
            + raw_file_name
            + ","
            + file_name
            + ","
            + status
    )
    return row


def generate_bucket_file_list(source):
    # get_variables()
    all_blobs = list_blobs_in_a_path(
        bucket_name, integration_processed_path + "/" + source + "/"
    )
    output_file = open(source + bucket_file_list, "w")
    output_file.write(
        "bucket_file_path"
        + ","
        + "source"
        + ","
        + "audio_id"
        + ","
        + "raw_file_name"
        + ","
        + "utterances_file_name"
        + ","
        + "status"
    )
    for blob in all_blobs:
        full_path = str(blob.name).replace(",", "")
        try:
            if UTTERANCE_FILE_FORMAT in full_path:
                (
                    file_name,
                    raw_file_name,
                    source,
                    audio_id,
                    status,
                ) = get_prefix_attributes_full_path(
                    full_path, integration_processed_path
                )
                row = generate_row(
                    full_path, file_name, raw_file_name, source, audio_id, status
                )
                output_file.write("\n")
                output_file.write(row)
        except BaseException:
            print(f"Failed at {full_path}")
    print("Bucket list has been generated")
    output_file.close()


def cleanse_catalog(data_catalog_raw):
    data_catalog_raw = data_catalog_raw[~data_catalog_raw.audio_id.isna()]
    data_catalog_raw["raw_file_name"] = data_catalog_raw.raw_file_name.apply(
        lambda x: x.replace(",", "")
    )
    data_catalog_raw["audio_id"] = data_catalog_raw["audio_id"].astype("int")
    return data_catalog_raw


def fetch_data_catalog(source, db_catalog_tbl, db_conn_obj):
    if source == "":
        filter_string = "1=1"
    else:
        filter_string = f"source='{source}'"
    data_catalog_raw = pd.read_sql(
        f"SELECT * FROM {db_catalog_tbl} where {filter_string}", db_conn_obj
    )
    data_catalog_raw = cleanse_catalog(data_catalog_raw)
    return data_catalog_raw


def cleanse_mapping_catalog(data_mapping_raw):
    data_mapping_raw = data_mapping_raw[~data_mapping_raw.audio_id.isna()]
    data_mapping_raw["clipped_utterance_file_name"] = data_mapping_raw.clipped_utterance_file_name.apply(
        lambda x: x.replace(",", "")
    )
    data_mapping_raw["audio_id"] = data_mapping_raw["audio_id"].astype("int")
    return data_mapping_raw


def fetch_mapping_catalog(db_mapping_tbl, unique_audio_ids, db_conn_obj):
    filter_string = f"audio_id in {unique_audio_ids}"
    data_mapping_raw = pd.read_sql(
        f"SELECT * FROM {db_mapping_tbl} where {filter_string}", db_conn_obj
    )
    data_mapping_raw = cleanse_mapping_catalog(data_mapping_raw)
    return data_mapping_raw


def fetch_bucket_list(source, bucket_file_list):
    generate_bucket_file_list(source)
    data_bucket_raw = pd.read_csv(source + bucket_file_list, low_memory=False)
    data_bucket_raw["audio_id"] = data_bucket_raw["audio_id"].astype("int")
    os.remove(source + bucket_file_list)
    return data_bucket_raw


def get_bucket_list_not_in_catalog(data_catalog_exploded, data_bucket_raw):
    bucket_list_not_in_catalog = data_bucket_raw.merge(
        data_catalog_exploded,
        how="left",
        on=["audio_id", "utterances_file_name"],
        suffixes=(None, "_y"),
    )
    return bucket_list_not_in_catalog[
        bucket_list_not_in_catalog["raw_file_name_y"].isna()
    ]
    # return bucket_list_not_in_catalog


def get_catalog_list_not_in_bucket(data_catalog_exploded, data_bucket_raw):
    catalog_list_not_in_bucket = data_bucket_raw.merge(
        data_catalog_exploded,
        how="right",
        on=["audio_id", "utterances_file_name"],
        suffixes=("_x", None),
    )
    return catalog_list_not_in_bucket[
        catalog_list_not_in_bucket["raw_file_name_x"].isna()
    ]
    # return catalog_list_not_in_bucket


def parse_json_utterance_meta(json_data):
    json_dict = json.loads(json_data)
    return (
            str(json_dict["name"]).replace(",", "")
            + ","
            + str(json_dict["duration"])
            + ","
            + json_dict["status"]
    )


def check_json_utterance_meta(json_data):
    # print(f"Utterance_file_list being processed is {jsonData}")
    # try:
    #     json.loads(jsonData)
    # except (ValueError, TypeError):
    #     return False
    # return True
    if isinstance(json_data, dict):
        return True
    else:
        return False


def parse_string_utterance_meta(data):
    data = data.replace(",", "")
    filename = data.split(":")[0]
    duration = data.split(":")[-1]
    status = ""
    return filename + "," + str(duration) + "," + status


def convert_string_utterance_meta(data):
    dicti = {}
    data = data.replace(",", "")
    filename = data.split(":")[0]
    duration = data.split(":")[-1]
    status = np.nan
    snr = np.nan
    reason = np.nan
    dicti["name"] = filename
    dicti["duration"] = duration
    dicti["snr_value"] = snr
    dicti["status"] = status
    dicti["reason"] = reason
    return json.dumps(dicti)


def sanitize_snr_formats(record):
    list_of_cleaned_utterances = []
    for utterance in json.loads(record):
        try:
            if str(utterance.get("snr_value")) == "nan":
                utterance["snr_value"] = 0.0
                list_of_cleaned_utterances.append(utterance)
            else:
                utterance["snr_value"] = float(utterance.get("snr_value", 0))
                list_of_cleaned_utterances.append(utterance)

        except TypeError:
            utterance["snr_value"] = 0.0
            list_of_cleaned_utterances.append(utterance)

        except ValueError:
            utterance["snr_value"] = 0.0
            list_of_cleaned_utterances.append(utterance)
    return list_of_cleaned_utterances


def convert_str_to_list(record):
    try:
        return ast.literal_eval(record)
    except ValueError as error:
        print(error)
        print(record)
        clean_record = sanitize_snr_formats(record)

        return clean_record


def explode_utterances(data_catalog_raw):
    data_catalog_raw["utterances_files_list"].fillna("[]", inplace=True)
    data_catalog_raw.utterances_files_list = (
        data_catalog_raw.utterances_files_list.astype("str").apply(convert_str_to_list)
    )
    data_catalog_exploded = data_catalog_raw.explode(
        "utterances_files_list"
    ).reset_index(drop=True)
    return data_catalog_exploded


def normalize_exploded_utterances(data_catalog_raw):
    data_catalog_exploded = explode_utterances(data_catalog_raw)
    utterances_files_list_meta = data_catalog_exploded.utterances_files_list.apply(
        lambda x: parse_json_utterance_meta(json.dumps(x))
        if check_json_utterance_meta(x)
        else parse_string_utterance_meta(str(x))
    )
    utterances_files_list_meta = utterances_files_list_meta.str.split(",", expand=True)
    # print(utterances_files_list_meta)
    data_catalog_exploded[
        "utterances_files_list"
    ] = data_catalog_exploded.utterances_files_list.apply(
        lambda x: json.dumps(x)
        if check_json_utterance_meta(x)
        else convert_string_utterance_meta(str(x))
    )
    data_catalog_exploded.insert(
        5, "utterances_file_name", utterances_files_list_meta[0]
    )
    data_catalog_exploded.insert(
        6, "utterances_file_duration", utterances_files_list_meta[1].astype("float")
    )
    data_catalog_exploded.insert(
        7, "utterances_file_status", utterances_files_list_meta[2]
    )
    return data_catalog_exploded


def get_bucket_list_in_catalog(data_catalog_exploded, data_bucket_raw):
    bucket_list_in_catalog = data_bucket_raw.merge(
        data_catalog_exploded,
        how="inner",
        on=["audio_id", "utterances_file_name"],
        suffixes=("_x", None),
    )
    return bucket_list_in_catalog


def get_invalid_utterance_duration(bucket_list_in_catalog):
    return bucket_list_in_catalog[
        ~bucket_list_in_catalog.utterances_file_duration.between(0.5, 15)
    ].sort_values("raw_file_name")


def append_file_and_duration(valid_utterance_duration):
    valid_utterance_duration["utterances_files_list"] = valid_utterance_duration[
        ["utterances_files_list", "utterances_file_duration"]
    ].apply(lambda x: x[0] + ":" + str(x[1]), axis=1)
    return valid_utterance_duration


def merge_with_mapping_catalog(df_cleaned_dataset, unique_audio_ids):
    db_mapping_catalog = fetch_mapping_catalog(db_mapping_tbl, unique_audio_ids, get_db_connection_object())
    db_mapping_catalog = db_mapping_catalog[
        ['audio_id', 'clipped_utterance_file_name', 'speaker_id', 'speaker_gender', 'snr', 'status',
         'staged_for_transcription', 'language_confidence_score', 'was_noise']]
    df_cleaned_dataset['utterance_file_name'] = df_cleaned_dataset.wav_path_bucket.apply(lambda x: x.split('/')[-1])
    df_cleaned_dataset_mapped = df_cleaned_dataset.loc[:, df_cleaned_dataset.columns != 'speaker_gender'].merge(
        db_mapping_catalog, left_on=["audio_id", "utterance_file_name"],
        right_on=["audio_id", "clipped_utterance_file_name"], suffixes=("", "_y"))
    return df_cleaned_dataset_mapped.loc[:, df_cleaned_dataset_mapped.columns != 'clipped_utterance_file_name']


def get_valid_utterance_duration_unexploded(bucket_list_in_catalog):
    valid_utterance_duration = bucket_list_in_catalog[
        bucket_list_in_catalog.utterances_file_duration.between(0.5, 15)
    ].sort_values("raw_file_name")
    # valid_utterance_duration = append_file_and_duration(valid_utterance_duration)
    valid_utterance_duration = valid_utterance_duration[
        valid_utterance_duration.status.str.lower() != "rejected"
        ]
    valid_utterance_duration = valid_utterance_duration[
        valid_utterance_duration.utterances_file_status.str.lower() != "rejected"
        ]
    return valid_utterance_duration.groupby("audio_id", as_index=False).agg(
        {
            "utterances_file_duration": lambda x: x.sum() / 60,
            "utterances_files_list": lambda tdf: tdf.tolist(),
        }
    )


def get_duplicates_utterances(data_catalog_raw):
    return data_catalog_raw[
        data_catalog_raw.duplicated(subset=["raw_file_name"], keep=False)
    ].sort_values(by="raw_file_name")


def get_unique_utterances(data_catalog_raw):
    return data_catalog_raw[~data_catalog_raw.duplicated(subset=["raw_file_name"])]


def get_valid_and_unique_utterances(
        df_catalog_unique, df_catalog_valid_utterance_duration_unexploded
):
    df_valid_utterances_with_unique_audioid = (
        df_catalog_valid_utterance_duration_unexploded.merge(
            df_catalog_unique, on="audio_id", suffixes=("", "_y")
        )
    )
    df_valid_utterances_with_unique_audioid[
        "cleaned_duration"
    ] = df_valid_utterances_with_unique_audioid["utterances_file_duration"]

    df_valid_utterances_with_unique_audioid.drop(
        ["utterances_files_list_y", "utterances_file_duration"], axis=1, inplace=True
    )
    return df_valid_utterances_with_unique_audioid


# def get_bucket_list_in_catalog_unexploded(bucket_list_in_catalog, data_catalog_raw):
#     audio_ids_in_bucket_and_catalog = bucket_list_in_catalog[~bucket_list_in_catalog.
#     duplicated(subset=['audio_id'])][[
#         'audio_id']]
#     return data_catalog_raw.merge(audio_ids_in_bucket_and_catalog,
#                                   on='audio_id')
#


def append_transcription_files_list(df_cleaned_dataset, stage):
    df_cleaned_dataset_transcipts = df_cleaned_dataset.copy()
    df_cleaned_dataset_transcipts.bucket_file_path = (
        df_cleaned_dataset_transcipts.bucket_file_path.apply(
            lambda x: x.replace(UTTERANCE_FILE_FORMAT, TRANSCRIPT_FILE_FORMAT)
        )
    )
    df_cleaned_dataset_transcipts.utterances_file_name = (
        df_cleaned_dataset_transcipts.utterances_file_name.apply(
            lambda x: x.replace(UTTERANCE_FILE_FORMAT, TRANSCRIPT_FILE_FORMAT)
        )
    )
    if "post" in stage:
        df_cleaned_dataset.insert(
            loc=1,
            column="transcriptions_path_bucket",
            value=df_cleaned_dataset_transcipts["bucket_file_path"].values.tolist(),
        )
        df_cleaned_dataset.transcriptions_path_bucket = (
            df_cleaned_dataset.transcriptions_path_bucket.apply(
                lambda x: "gs://" + bucket_name + "/" + x
            )
        )
    df_cleaned_dataset = df_cleaned_dataset.drop(["utterances_file_name"], axis=1)
    df_cleaned_dataset = df_cleaned_dataset.rename(
        columns={
            "bucket_file_path": "wav_path_bucket",
            "utterances_file_duration": "duration",
        }
    )
    df_cleaned_dataset.wav_path_bucket = df_cleaned_dataset.wav_path_bucket.apply(
        lambda x: "gs://" + bucket_name + "/" + x
    )

    return df_cleaned_dataset


def generate_cleaned_dataset(df_cleaned_utterances_exploded, bucket_list_in_catalog, stage):
    df_cleaned_dataset = df_cleaned_utterances_exploded.merge(
        bucket_list_in_catalog,
        on=["audio_id", "utterances_files_list"],
        suffixes=("_y", " "),
    )
    return append_transcription_files_list(
        df_cleaned_dataset[
            ["audio_id", "bucket_file_path", "utterances_file_name", "utterances_file_duration"]
        ], stage
    )


def get_audio_ids(df_catalog_unique):
    return tuple(df_catalog_unique['audio_id'].tolist())


def generate_data_validation_report(data_catalog_raw, data_bucket_raw, stage):
    print("Generate reports...")
    data_catalog_exploded = normalize_exploded_utterances(data_catalog_raw)
    bucket_list_not_in_catalog = get_bucket_list_not_in_catalog(
        data_catalog_exploded, data_bucket_raw
    )
    catalog_list_not_in_bucket = get_catalog_list_not_in_bucket(
        data_catalog_exploded, data_bucket_raw
    )
    bucket_list_in_catalog = get_bucket_list_in_catalog(
        data_catalog_exploded, data_bucket_raw
    )
    bucket_list_in_catalog_cleaned = bucket_list_in_catalog[
        bucket_list_in_catalog.status == "clean"
        ]
    df_catalog_duplicates = get_duplicates_utterances(data_catalog_raw)
    df_catalog_unique = get_unique_utterances(data_catalog_raw)

    unique_audio_ids = get_audio_ids(df_catalog_unique)

    df_catalog_invalid_utterance_duration = get_invalid_utterance_duration(
        bucket_list_in_catalog_cleaned
    )
    df_catalog_valid_utterance_duration_unexploded = (
        get_valid_utterance_duration_unexploded(bucket_list_in_catalog_cleaned)
    )

    df_valid_utterances_with_unique_audioid = get_valid_and_unique_utterances(
        df_catalog_unique, df_catalog_valid_utterance_duration_unexploded
    )

    if "pre" in stage:
        writer = pd.ExcelWriter(report_file_name, engine="xlsxwriter")
        bucket_list_not_in_catalog.astype({"audio_id": "str"}).to_excel(
            writer, sheet_name="bucket_list_not_in_catalog", index=False
        )
        catalog_list_not_in_bucket.astype({"audio_id": "str"}).to_excel(
            writer, sheet_name="catalog_list_not_in_bucket", index=False
        )
        bucket_list_in_catalog.astype({"audio_id": "str"}).to_excel(
            writer, sheet_name="catalog_list_in_bucket", index=False
        )
        # catalog_list_with_rejected_status.to_excel(writer,
        # sheet_name='catalog_list_rejected_ones', index=False)
        df_catalog_invalid_utterance_duration.astype({"audio_id": "str"}).to_excel(
            writer, sheet_name="catalog_list_invalid_duration", index=False
        )
        df_catalog_duplicates.astype({"audio_id": "str"}).to_excel(
            writer, sheet_name="catalog_list_with_duplicates", index=False
        )
        df_valid_utterances_with_unique_audioid.astype({"audio_id": "str"}).to_excel(
            writer, sheet_name="Cleaned_data_catalog", index=False
        )
        writer.save()
        print(f"{report_file_name} has been generated....")
    df_cleaned_utterances_exploded = explode_utterances(
        df_valid_utterances_with_unique_audioid
    )
    df_cleaned_dataset = generate_cleaned_dataset(
        df_cleaned_utterances_exploded, bucket_list_in_catalog_cleaned, stage
    )
    df_cleaned_dataset_mapped = merge_with_mapping_catalog(df_cleaned_dataset, unique_audio_ids)
    df_cleaned_dataset_mapped.to_csv(cleaned_csv_report_file_name, index=False)
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
    if os.path.exists(report_file_name):
        upload_blob(
            bucket_name,
            report_file_name,
            os.path.join(report_upload_path, report_file_name),
        )
        os.remove(report_file_name)
    if os.path.exists(cleaned_csv_report_file_name):
        upload_blob(
            bucket_name,
            cleaned_csv_report_file_name,
            os.path.join(
                report_upload_path, "Final_csv_reports", cleaned_csv_report_file_name
            ),
        )
        os.remove(cleaned_csv_report_file_name)


def __load_yaml_file():
    read_dict = {}
    with open(config_path, "r") as file:
        read_dict = yaml.safe_load(file)
    return read_dict


def create_db_engine():
    config_file = __load_yaml_file()
    db_configuration = config_file["db_configuration"]
    db_name = db_configuration["db_name"]
    db_user = db_configuration["db_user"]
    db_pass = db_configuration["db_pass"]
    cloud_sql_connection_name = db_configuration["cloud_sql_connection_name"]
    db = create_engine(
        f"postgresql://{db_user}:{db_pass}@{cloud_sql_connection_name}/{db_name}"
    )
    return db


def get_db_connection_object():
    return create_db_engine()


def check_dataframes(data_catalog_raw, data_bucket_raw):
    catalouge_len = len(data_catalog_raw)
    bucket_len = len(data_bucket_raw)
    if catalouge_len == 0 and bucket_len != 0:
        print("For the given source no data in catalouge")
        sys.exit(1)
    elif catalouge_len != 0 and bucket_len == 0:
        print("For the given source no data in bucket")
        sys.exit(1)
    elif catalouge_len == 0 and bucket_len == 0:
        print("For the given source no data found. Check the path and source name!!!!")
        sys.exit(1)
    else:
        pass


def report_generation_pipeline(stage, bucket, mode="cluster", sources={}):
    if mode == "local":
        get_local_variables(bucket, sources)
    else:
        get_airflow_variables(stage)
    if not bool(validation_report_sources):
        print("No sources has been specified in the source list,so Aborting....")
        exit(1)
    for _source, _source_info in validation_report_sources.items():
        print(f"Processing for source: {_source}")
        try:
            language = _source_info.get('language').lower()
        except KeyError as e:
            print(f"No language parameter provided for source {_source},Aborting...")
            exit(0)
        get_common_variables(stage, language)
        set_report_names(_source)
        data_catalog_raw, data_bucket_raw = fetch_data(
            _source, get_db_connection_object()
        )
        check_dataframes(data_catalog_raw, data_bucket_raw)
        generate_data_validation_report(data_catalog_raw, data_bucket_raw, stage)
        upload_report_to_bucket()
        print(f"Processed successfully for source: {_source}")


if __name__ == "__main__":
    report_generation_pipeline(
        mode="local",
        stage="post-transcription",
        bucket="ekstepspeechrecognition-dev",
        # language="hindi",
        sources={
            "STAGE": {
                "language": "hindi"
            }}
    )
