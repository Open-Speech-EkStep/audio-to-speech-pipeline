import datetime
import json
import re
import os
import pandas as pd
from gcs_utils import list_blobs_in_a_path, copy_blob, check_blob, \
    move_blob, upload_blob, read_blob, move_directory, download_blob

from airflow.models import Variable


class mydict(dict):
    def __str__(self):
        return json.dumps(self)


def get_variables():
    global source_chunk_path
    global bucket_name
    global archive_utterances_path
    global integration_processed_path
    global experiment_output
    source_chunk_path = Variable.get("utteranceschunkpath")
    archive_utterances_path = Variable.get("archiveutterancespath")
    integration_processed_path = Variable.get("integrationprocessedpath")
    experiment_output = Variable.get("experimentoutput")
    bucket_name = Variable.get("bucket")


def count_utterances_file_chunks(**kwargs):
    get_variables()
    utterances_names = json.loads(Variable.get("utteranceschunkslist"))
    all_blobs = list_blobs_in_a_path(bucket_name, source_chunk_path)
    list_of_blobs = []
    for blob in all_blobs:
        if blob.name.endswith('.csv'):
            list_of_blobs.append(str(blob.name))
    print("***The utterances file chunks***", list_of_blobs)
    utterances_names["utteranceschunkslist"] = list_of_blobs
    utterances_names = mydict(utterances_names)
    Variable.set("utteranceschunkslist", utterances_names)


def move_utterance_chunk(bucket_name, source_file_name, experiment_name):
    get_variables()
    archive_utterances_file_name = archive_utterances_path + experiment_name + "/" + source_file_name.split('/')[
        -1]
    move_blob(bucket_name, source_file_name, bucket_name, archive_utterances_file_name)
    # os.remove(local_file_name)


def copy_utterances(src_file_name, **kwargs):
    get_variables()
    try:
        extn_list = ['wav', 'txt']
        local_file_name = bucket_name.split('/')[-1]
        download_blob(bucket_name, src_file_name, local_file_name)
        df = pd.read_csv(local_file_name)
        for i, row in df.iterrows():
            audio_id = row["audio_id"]
            source = row["source"]
            experiment_name = row["experiment_name"]
            file_name = row["clipped_utterance_file_name"]
            utterance_file_name = str(file_name).split(".")[0]
            print(file_name, utterance_file_name, audio_id)
            for extn in extn_list:
                source_blob_name = integration_processed_path + source.lower() + "/" + str(
                    audio_id) + "/clean/" + utterance_file_name + "." + extn
                destination_blob_name = experiment_output + experiment_name + "/" + str(
                    audio_id) + "/" + utterance_file_name + "." + extn
                copy_blob(
                    bucket_name=bucket_name,
                    blob_name=source_blob_name,
                    destination_bucket_name=bucket_name,
                    destination_blob_name=destination_blob_name,
                )
        move_utterance_chunk(bucket_name, src_file_name, experiment_name)

    finally:
        print("All files have been copied")


if __name__ == "__main__":
    pass
