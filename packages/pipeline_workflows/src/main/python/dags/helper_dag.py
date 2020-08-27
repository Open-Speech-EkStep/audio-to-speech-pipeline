import datetime
import json
import re
import os

from gcs_utils import list_blobs_in_a_path, copy_blob, check_blob, \
    move_blob, upload_blob, read_blob, move_directory
from airflow.models import Variable

snr_done_path = Variable.get("snrdonepath")
stt_input_dir_path = Variable.get("snrdonepath")
bucket_name = Variable.get("bucket")


def get_variables():
    global bucket_name
    global meta_file_extention
    bucket_name = Variable.get("bucket")
    meta_file_extention = Variable.get("metafileextension")


class mydict(dict):
    def __str__(self):
        return json.dumps(self)


def get_file_name(file_prefix_name, delimiter):
    return file_prefix_name.split(delimiter)[-1]


def get_file_extension(file_name):
    return file_name.split('.')[-1]


def get_metadata_file_name(file_name):
    return '.'.join(file_name.split('.')[:-1]) + meta_file_extention


def check_if_meta_data_present(full_source_path, metadata_file_name):
    global bucket_name
    return check_blob(bucket_name, full_source_path + '/' + metadata_file_name)


def get_files_path_with_prefix(file_path, file_prefix, file_name):
    return file_path + '/' + str(file_prefix) + '/' + file_name


def get_files_path_with_no_prefix(file_path, file_name):
    return file_path + '/' + file_name


def get_audio_id():
    return datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')[:-2]


def update_metadata_file(source, audio_id):
    with open(source + "_audio_id.txt", "a+") as myfile:
        myfile.write(audio_id + '\n')


def create_empty_file(source):
    with open(source + "_audio_id.txt", "a+") as myfile:
        myfile.write("")


def move_metadata_file(source, tobe_processed_path):
    global bucket_name
    source_file_name = source + "_audio_id.txt"
    destination_blob_name = tobe_processed_path + source + \
        '/audio_id/' + get_audio_id() + '/' + source + "_audio_id.txt"
    if os.path.isfile(source_file_name):
        upload_blob(bucket_name, source_file_name, destination_blob_name)
        os.remove(source_file_name)
    else:
        create_empty_file(source)
        move_metadata_file(source, tobe_processed_path)


def condition_file_name(file_name):
    file_name_cleansed = file_name.translate({ord(i): None for i in "()&'"})
    file_name_split = file_name_cleansed.split('.')
    return '_'.join(file_name_split[:-1]) + '.' + file_name_split[-1]


def get_files_from_landing_zone(source, source_landing_path, error_landing_path, tobe_processed_path, batch_count, audio_format):
    get_variables()
    delimiter = "/"
    print("****The source is *****" + source)
    # meta_data_flag = False
    all_blobs = list_blobs_in_a_path(
        bucket_name, source_landing_path + source + delimiter)
    try:
        for blob in all_blobs:
            print("*********The file name is ********* " + blob.name)
            file_name = get_file_name(blob.name, delimiter)
            file_extension = get_file_extension(file_name)
            expected_file_extension = audio_format

            if file_extension in [expected_file_extension, expected_file_extension.swapcase()]:

                if batch_count > 0:
                    metadata_file_name = get_metadata_file_name(file_name)
                    print("File is {}".format(file_name))
                    audio_id = get_audio_id()

                    source_file_name = get_files_path_with_no_prefix(
                        source_landing_path + source, file_name)
                    source_meta_file_name = get_files_path_with_no_prefix(source_landing_path + source,
                                                                          metadata_file_name)
                    destination_file_name = get_files_path_with_prefix(tobe_processed_path + source, audio_id,
                                                                       condition_file_name(file_name))
                    destination_meta_file_name = get_files_path_with_prefix(tobe_processed_path + source, audio_id,
                                                                            condition_file_name(metadata_file_name))
                    if (check_if_meta_data_present(source_landing_path + source, metadata_file_name)):

                        print("Meta file {} is present".format(
                            metadata_file_name))
                        # meta_data_flag = True
                        move_blob(bucket_name, source_file_name,
                                  bucket_name, destination_file_name)
                        move_blob(bucket_name, source_meta_file_name,
                                  bucket_name, destination_meta_file_name)

                        update_metadata_file(source, audio_id)
                    else:
                        print("Meta file {} is not present,Moving to error....".format(
                            metadata_file_name))

                        error_destination_file_name = get_files_path_with_no_prefix(error_landing_path + source,
                                                                                    file_name)

                        move_blob(bucket_name, source_file_name,
                                  bucket_name, error_destination_file_name)

                else:
                    break
                batch_count -= 1
    finally:
        move_metadata_file(source, tobe_processed_path)


def get_latest_file_from_bucket(source_path):
    global bucket_name
    delimiter = "/"

    all_blobs = list_blobs_in_a_path(bucket_name, source_path, delimiter)

    for blob in all_blobs:
        pass

    if delimiter:
        list_prefixes = []
        for prefix in all_blobs.prefixes:
            list_prefixes.append(int(prefix.split(delimiter)[-2:-1][0]))
            list_prefixes.sort(reverse=True)
        print("Latest date_time prefix is: {} ".format(list_prefixes[0]))
    return str(list_prefixes[0])


def get_audio_id_path(tobe_processed_path, source):
    return tobe_processed_path + source + '/' + "audio_id" + '/'


def get_audio_id_blob_name(latest_audio_id_path, source):
    return latest_audio_id_path + '/' + source + '_' + "audio_id.txt"


def get_audio_ids(source, tobe_processed_path, **kwargs):
    get_variables()
    audio_file_ids = json.loads(Variable.get("audiofileids"))
    audio_id_source_path = get_audio_id_path(tobe_processed_path, source)
    latest_audio_id_path = audio_id_source_path + \
        get_latest_file_from_bucket(audio_id_source_path)
    audio_id_blob = get_audio_id_blob_name(latest_audio_id_path, source)

    if check_blob(bucket_name, audio_id_blob):
        audio_ids = read_blob(bucket_name, audio_id_blob).splitlines()
        audio_file_ids[source] = audio_ids
        audio_file_ids = mydict(audio_file_ids)
        Variable.set("audiofileids", audio_file_ids)
    else:
        audio_file_ids[source] = []
        audio_file_ids = mydict(audio_file_ids)
        Variable.set("audiofileids", audio_file_ids)

def get_require_audio_id(source,stt_source_path,batch_count):

    audio_ids = json.loads(Variable.get("audioidsforstt"))

    source_dir_path = f'{stt_source_path}{source}'
    all_audio_id_path = list_blobs_in_a_path(bucket_name,source_dir_path)
    audio_id_list = []
    for blob in all_audio_id_path:

        if batch_count > 0:
            if ".wav" in blob.name:
                audio_id = blob.name.split('/')[-3]
    
                audio_id_list.append(audio_id)
                batch_count = batch_count - 1
        else:
            break
    audio_ids[source] = list(set(audio_id_list))

    Variable.set("audioidsforstt", mydict(audio_ids))


def move_raw_to_processed(source, batch_audio_file_ids, tobe_processed_path, **kwargs):
    get_variables()
    for audio_id in batch_audio_file_ids:
        source_path = tobe_processed_path + source + '/' + audio_id
        destination_path = snr_done_path + source + '/' + audio_id
        move_directory(bucket_name, source_path, destination_path)


if __name__ == "__main__":
    pass
