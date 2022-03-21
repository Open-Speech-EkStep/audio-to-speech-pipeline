import collections
import json
import os
from datetime import datetime
from operator import itemgetter

import pandas as pd
import yaml
from airflow.models import Variable
from gcs_utils import (
    check_blob
)
from gcs_utils import list_blobs_in_a_path, upload_blob, download_blob
from sqlalchemy import create_engine


class MyDict(dict):
    def __str__(self):
        return json.dumps(self)


def get_file_name(file_prefix_name, delimiter):
    return file_prefix_name.split(delimiter)[-1]


def get_file_extension(file_name):
    return file_name.split(".")[-1]


def get_metadata_file_name(file_name, meta_file_extension):
    return ".".join(file_name.split(".")[:-1]) + meta_file_extension


def check_if_meta_data_present(full_source_path, metadata_file_name, bucket_name):
    return check_blob(bucket_name, full_source_path + "/" + metadata_file_name)


def condition_file_name(file_name):
    file_name_cleansed = file_name.translate({ord(i): None for i in "()&'"})
    file_name_split = file_name_cleansed.split(".")
    return "_".join(file_name_split[:-1]) + "." + file_name_split[-1]


def get_sorted_file_list_after_batch(file_name_dict, batch_count):
    file_name_dict_sorted = collections.OrderedDict(
        sorted(file_name_dict.items(), key=itemgetter(1))
    )
    print(f"The sorted audio_ids as per their size {file_name_dict_sorted}")
    file_name_sorted_list = list(file_name_dict_sorted.keys())
    if len(file_name_sorted_list) > batch_count:
        return file_name_sorted_list[:batch_count]
    return file_name_sorted_list


def get_file_path_from_bucket(
        source,
        source_landing_path,
        batch_count,
        audio_format,
        meta_file_extension,
        bucket_name,
):
    file_path_dict = json.loads(Variable.get("audiofilelist"))
    file_name_dict = {}

    delimiter = "/"
    print("****The source is *****" + source)

    all_blobs = list_blobs_in_a_path(
        bucket_name, source_landing_path + source + delimiter
    )

    for blob in all_blobs:
        print("*********The file name is ********* " + blob.name)
        print("*********The file size is {} bytes *********".format(blob.size))
        file_size = blob.size
        file_name = get_file_name(blob.name, delimiter)
        print(file_name,"*********filename*********")
        if file_name is None or len(file_name.strip())==0:
            raise ValueError('file not found')

        file_extension = get_file_extension(file_name)
        expected_file_extension = audio_format

        if file_extension in [
            expected_file_extension,
            expected_file_extension.swapcase(),
        ]:
            metadata_file_name = get_metadata_file_name(file_name, meta_file_extension)
            print("File is {}".format(file_name))
            print("Meta File is {}".format(metadata_file_name))

            if check_if_meta_data_present(
                    source_landing_path + source, metadata_file_name, bucket_name
            ):
                file_name_dict[file_name] = file_size

    file_path_dict[source] = get_sorted_file_list_after_batch(
        file_name_dict, batch_count
    )
    file_path_dict = MyDict(file_path_dict)
    Variable.set("audiofilelist", file_path_dict)


def get_require_audio_id(source, stt_source_path, data_set, batch_count, bucket_name):
    audio_ids = json.loads(Variable.get("audioidsforstt"))

    source_dir_path = f"{stt_source_path}{source}/{data_set}"
    all_audio_id_path = list_blobs_in_a_path(bucket_name, source_dir_path)
    audio_id_list = set()
    for blob in all_audio_id_path:

        if batch_count > 0:
            if ".wav" in blob.name:
                audio_id = blob.name.split("/")[-3]

                if audio_id not in audio_id_list:
                    audio_id_list.add(audio_id)
                    batch_count = batch_count - 1
        else:
            break
    audio_ids[source] = list(audio_id_list)

    Variable.set("audioidsforstt", MyDict(audio_ids))


def parse_config_file():
    config_file = __load_yaml_file()["config"]['common']
    db_configuration = config_file["db_configuration"]
    db_name = db_configuration["db_name"]
    db_user = db_configuration["db_user"]
    db_pass = db_configuration["db_pass"]
    cloud_sql_connection_name = db_configuration["cloud_sql_connection_name"]
    return db_name, db_user, db_pass, cloud_sql_connection_name


def create_db_engine():
    db_name, db_user, db_pass, cloud_sql_connection_name = parse_config_file()

    db = create_engine(
        f"postgresql://{db_user}:{db_pass}@{cloud_sql_connection_name}/{db_name}"
    )
    return db


def __load_yaml_file():
    read_dict = {}
    with open(config_path, "r") as file:
        read_dict = yaml.safe_load(file)
    return read_dict


def get_db_connection_object():
    return create_db_engine()


def download_config_file(bucket_name):
    global config_path
    config_path = f"./config.yaml"
    print("Downloading config file")
    download_blob(
        bucket_name,
        f"data/audiotospeech/config/config.yaml",
        config_path, )


def fetch_require_audio_ids_for_stt(source, language, stt, data_set, bucket_name):
    audio_ids = json.loads(Variable.get("audioidsforstt"))
    download_config_file(bucket_name)
    if data_set not in ('train', 'test', ''):
        print("Enter a valid data set type and rerun")
        exit(1)
    data_catalog_raw = fetch_data_catalog(source, language.title(), data_set, stt,
                                          get_db_connection_object())
    # audio_ids[source] = list(data_catalog_raw.audio_id)
    audio_ids[source] = data_catalog_raw
    print(audio_ids[source])
    Variable.set("audioidsforstt", MyDict(audio_ids))


def upload_report_to_bucket(bucket_name, language, source, report_file_name):
    # get_variables()
    print("Uploading report to bucket ...")
    if os.path.exists(report_file_name):
        upload_blob(
            bucket_name,
            report_file_name,
            os.path.join(f'data/data_snapshots/{language}/{source}/', report_file_name),
        )
        os.remove(report_file_name)
        print(f"Uploaded report {report_file_name} to bucket ...")


def fetch_db_data_dump(source, language, db_conn_obj):
    filter_string = f"audio_id in (select audio_id from media_metadata_staging where source = '{source}' and language = '{language}') and status= 'Clean' and staged_for_transcription = true"
    data_catalog_raw = pd.read_sql(
        f"SELECT speaker_id, clipped_utterance_duration, snr, speaker_gender FROM media_speaker_mapping where {filter_string}",
        db_conn_obj
    )
    data_catalog_raw = cleanse_catalog(data_catalog_raw)
    return data_catalog_raw


def fetch_upload_db_data_dump(bucket_name, source, language):
    now = datetime.now()
    date_time = now.strftime("%m_%d_%Y_%H_%M_%S")
    report_file_name = f"Data_dump_snapshot_{date_time}_{source}_{language}.xlsx"
    download_config_file(bucket_name)
    data_catalog_raw = fetch_db_data_dump(source, language, get_db_connection_object())
    writer = pd.ExcelWriter(report_file_name, engine="xlsxwriter")
    data_catalog_raw.astype({"audio_id": "str"}).to_excel(
        writer, sheet_name="data_dump_snapshot_catalog", index=False
    )
    writer.save()
    print(f"{report_file_name} has been generated....")
    upload_report_to_bucket(bucket_name, language, source, report_file_name)
    print(f"Finished data dump....")


def cleanse_catalog(data_catalog_raw):
    data_catalog_raw = data_catalog_raw[~data_catalog_raw.audio_id.isna()]
    data_catalog_raw["audio_id"] = data_catalog_raw["audio_id"].astype("str")
    return data_catalog_raw


def fetch_data_catalog(source, language, data_set, stt, db_conn_obj):
    filter_string = f"audio_id in (select audio_id from media_metadata_staging where source = '{source}' and language = '{language}' and (data_set_used_for IS NULL or data_set_used_for = '{data_set}')) and status= 'Clean' and '{stt}'!= ALL(stt_api_used) and staged_for_transcription = true"
    data_catalog_raw = pd.read_sql(
        f"SELECT distinct audio_id,COUNT(*) OVER (PARTITION BY audio_id) as utterance_count FROM media_speaker_mapping where {filter_string} order by utterance_count desc",
        db_conn_obj
    )
    data_catalog_raw = cleanse_catalog(data_catalog_raw)
    return dict(data_catalog_raw.values)


def data_marking_start():
    return "started.."


def audio_analysis_start():
    return "audio analysis started.."


def upload_batch(source, bucket_name, destination_path, batch_filename):
    upload_blob(
        bucket_name,
        batch_filename,
        os.path.join(destination_path, source, batch_filename),
    )


def split_upload_batches(source, bucket_name, destination_path, file_object, max_records_threshold_per_pod):
    lines_per_file = max_records_threshold_per_pod
    list_of_batches = []
    batchfile = None
    batch_file_count = 0
    file_object.seek(0)
    for lineno, line in enumerate(file_object):
        # print(lineno, " ", line)
        if lineno % lines_per_file == 0:
            batch_file_count += 1
            if batchfile:
                batchfile.close()
                print("Uploading batch: ", batch_filename)
                upload_batch(source, bucket_name, destination_path, batch_filename)
                list_of_batches.append(os.path.join(bucket_name, destination_path, source, batch_filename))
            batch_filename = 'batch_file_{}.txt'.format(batch_file_count)
            batchfile = open(batch_filename, "w")
        batchfile.write(line)
    if batchfile:
        batchfile.close()
        print("Uploading last batch: ", batch_filename)
        upload_batch(source, bucket_name, destination_path, batch_filename)
        list_of_batches.append(os.path.join(bucket_name, destination_path, source, batch_filename))
    return list_of_batches


def find_all_batch_without_npz(bucket_name, destination_path, source):
    all_file_list = []
    full_path_for_embeddings = os.path.join(destination_path, source)

    all_batch_txt_npz = list_blobs_in_a_path(
        bucket_name, full_path_for_embeddings
    )

    has_npz_file = False
    for filename in all_batch_txt_npz:
        print(filename.name)

        if 'txt' in filename.name:
            filename_without_extension = filename.name.replace('.txt', '')
            print(filename_without_extension, "when it is txt")
            if filename_without_extension in all_file_list:
                all_file_list.remove(filename_without_extension)
                continue
            all_file_list.append(filename_without_extension)
        if 'npz' in filename.name:
            has_npz_file = True
            filename_without_extension = filename.name.replace('.npz', '')
            print(filename_without_extension, "when it is npz")
            if filename_without_extension in all_file_list:
                all_file_list.remove(filename_without_extension)
                continue
            all_file_list.append(filename_without_extension)

    return all_file_list, has_npz_file


def generate_splitted_batches_for_audio_analysis(
        source,
        source_path,
        destination_path,
        max_records_threshold_per_pod,
        audio_format,
        bucket_name
):
    delimiter = "/"
    print("****The source is *****" + source)
    print("****The source path is *****" + source_path)
    print("****The destination path is *****" + destination_path)
    batch_file_path_dict = json.loads(Variable.get("embedding_batch_file_list"))

    all_batch_set, has_npz_file = find_all_batch_without_npz(bucket_name, destination_path, source)

    if len(all_batch_set) > 0:
        print(all_batch_set, "All batch set")
        # list_of_batches = list(all_batch_set)
        add_txt_in_path = [f'{bucket_name}/{file_path}.txt' for file_path in all_batch_set]

        batch_file_path_dict[source] = add_txt_in_path
        batch_file_path_dict = MyDict(batch_file_path_dict)
        Variable.set("embedding_batch_file_list", batch_file_path_dict)
        return

    if len(all_batch_set) == 0 and has_npz_file:
        batch_file_path_dict[source] = all_batch_set
        batch_file_path_dict = MyDict(batch_file_path_dict)
        Variable.set("embedding_batch_file_list", batch_file_path_dict)
        return

    all_blobs = list_blobs_in_a_path(
        bucket_name, source_path + source + delimiter
    )
    list_of_batches = []
    processed_flag = False
    if os.path.exists(source + ".txt"):
        os.remove(source + ".txt")
    with open(source + ".txt", "a+") as file_object:
        appendEOL = False
        file_object.seek(0)
        no_of_lines = 0
        data = file_object.read(100)
        if len(data) > 0:
            appendEOL = True
        for blob in all_blobs:
            # print("*********The file name is ********* " + blob.name)
            # print("*********The file size is {} bytes *********".format(blob.size))
            file_name = get_file_name(blob.name, delimiter)

            file_extension = get_file_extension(file_name)
            expected_file_extension = audio_format

            if file_extension == 'npz':
                print("Final embedding is present already for the source,No further chunking of embeddings needed")
                processed_flag = True
                break

            if file_extension in [
                expected_file_extension,
                expected_file_extension.swapcase(),
            ] and 'clean' in blob.name:

                if appendEOL == True:
                    file_object.write("\n")
                else:
                    appendEOL = True

                file_object.write(os.path.join(bucket_name, blob.name))
                no_of_lines += 1

        if no_of_lines > 0 and not processed_flag:
            print("Total number of audio files selected are : ", no_of_lines)
            print("split into batches and upload batches")
            list_of_batches = split_upload_batches(source, bucket_name, destination_path, file_object,
                                                   max_records_threshold_per_pod)
            list_of_batches
    batch_file_path_dict[source] = list_of_batches
    batch_file_path_dict = MyDict(batch_file_path_dict)
    Variable.set("embedding_batch_file_list", batch_file_path_dict)


# generate_splitted_batches_for_audio_analysis("Smart_money_with_Sonia_Shenoy",
#                                              "data/audiotospeech/raw/download/catalogued/indian_english/audio/",
#                                              "data/audiotospeech/raw/download/catalogued/indian_english/embeddings/", 500, "wav",
#                                              "ekstepspeechrecognition-dev")


# if __name__ == "__main__":
#     fetch_require_audio_ids_for_stt('247_tamil', 'tamil', 'ekstep', 'test',
#                                                        'ekstepspeechrecognition-test')
#
