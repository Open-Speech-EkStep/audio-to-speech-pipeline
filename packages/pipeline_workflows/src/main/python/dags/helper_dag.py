import datetime
import json
import os
import yaml
from operator import itemgetter
import collections
from gcs_utils import list_blobs_in_a_path, copy_blob, check_blob, \
    move_blob, upload_blob, read_blob, move_directory, download_blob
from airflow.models import Variable


class mydict(dict):
    def __str__(self):
        return json.dumps(self)


def get_file_name(file_prefix_name, delimiter):
    return file_prefix_name.split(delimiter)[-1]


def get_file_extension(file_name):
    return file_name.split('.')[-1]


def get_metadata_file_name(file_name, meta_file_extention):
    return '.'.join(file_name.split('.')[:-1]) + meta_file_extention


def check_if_meta_data_present(full_source_path, metadata_file_name, bucket_name):
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


def move_metadata_file(source, tobe_processed_path, bucket_name):
    source_file_name = source + "_audio_id.txt"
    destination_blob_name = tobe_processed_path + source + \
                            '/audio_id/' + get_audio_id() + '/' + source + "_audio_id.txt"
    if os.path.isfile(source_file_name):
        upload_blob(bucket_name, source_file_name, destination_blob_name)
        os.remove(source_file_name)
    else:
        create_empty_file(source)
        move_metadata_file(source, tobe_processed_path, bucket_name)


def condition_file_name(file_name):
    file_name_cleansed = file_name.translate({ord(i): None for i in "()&'"})
    file_name_split = file_name_cleansed.split('.')
    return '_'.join(file_name_split[:-1]) + '.' + file_name_split[-1]


def get_sorted_file_list_after_batch(file_name_dict, batch_count):
    file_name_dict_sorted = collections.OrderedDict(sorted(file_name_dict.items(), key=itemgetter(1)))
    print(f"The sorted audio_ids as per their size {file_name_dict_sorted}")
    file_name_sorted_list = list(file_name_dict_sorted.keys())
    if len(file_name_sorted_list) > batch_count:
        return file_name_sorted_list[:batch_count]
    return file_name_sorted_list


def get_file_path_from_bucket(source, source_landing_path, batch_count, audio_format, meta_file_extention, bucket_name):
    file_path_dict = json.loads(Variable.get("audiofilelist"))
    file_name_dict = {}

    delimiter = "/"
    print("****The source is *****" + source)

    all_blobs = list_blobs_in_a_path(
        bucket_name, source_landing_path + source + delimiter)

    for blob in all_blobs:
        print("*********The file name is ********* " + blob.name)
        print("*********The file size is {} bytes *********".format(blob.size))
        file_size = blob.size
        file_name = get_file_name(blob.name, delimiter)

        file_extension = get_file_extension(file_name)
        expected_file_extension = audio_format

        if file_extension in [expected_file_extension, expected_file_extension.swapcase()]:
            metadata_file_name = get_metadata_file_name(file_name, meta_file_extention, bucket_name)
            print("File is {}".format(file_name))
            print("Meta File is {}".format(metadata_file_name))

            if (check_if_meta_data_present(source_landing_path + source, metadata_file_name, bucket_name)):
                file_name_dict[file_name] = file_size

    file_path_dict[source] = get_sorted_file_list_after_batch(file_name_dict, batch_count)
    file_path_dict = mydict(file_path_dict)
    Variable.set("audiofilelist", file_path_dict)

def get_latest_file_from_bucket(source_path, bucket_name):
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


def get_require_audio_id(source, stt_source_path, batch_count, bucket_name):
    audio_ids = json.loads(Variable.get("audioidsforstt"))

    source_dir_path = f'{stt_source_path}{source}'
    all_audio_id_path = list_blobs_in_a_path(bucket_name, source_dir_path)
    audio_id_list = set()
    for blob in all_audio_id_path:

        if batch_count > 0:
            if ".wav" in blob.name:
                audio_id = blob.name.split('/')[-3]

                if audio_id not in audio_id_list:
                    audio_id_list.add(audio_id)
                    batch_count = batch_count - 1
        else:
            break
    audio_ids[source] = list(audio_id_list)

    Variable.set("audioidsforstt", mydict(audio_ids))


def __load_yaml_file(path):
    read_dict = {}
    with open(path, 'r') as file:
        read_dict = yaml.safe_load(file)
    return read_dict


if __name__ == "__main__":
    pass


def data_marking_start():
    return 'started..'


def audio_analysis_start():
    return 'audio analysis started..'
