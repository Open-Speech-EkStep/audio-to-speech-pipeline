import json

import pandas as pd
from ekstep_data_pipelines.common.dao.constants import (
    GET_UNIQUE_ID,
    IS_EXIST,
    COMMAND_WITH_LICENSE,
    COMMAND_WITHOUT_LICENSE,
    LICENSE,
)
from ekstep_data_pipelines.common.utils import get_logger

LOGGER = get_logger("CatalogueDao")


class CatalogueDao:
    def __init__(self, postgres_client):
        self.postgres_client = postgres_client

    def get_utterances(self, audio_id):
        parm_dict = {"audio_id": audio_id}
        utterances = self.postgres_client.execute_query(
            "select utterances_files_list from media_metadata_staging where audio_id = :audio_id",
            **parm_dict,
        )
        return json.loads(utterances[0][0]) if len(utterances) > 0 else []

    def get_utterances_by_source(self, source, status):
        parm_dict = {"source": source, "status": status}
        data = self.postgres_client.execute_query(
            "select speaker_id, clipped_utterance_file_name, clipped_utterance_duration, "
            "audio_id, snr "
            "from media_speaker_mapping "
            "where audio_id "
            'in (select audio_id from media_metadata_staging where "source" = :source) '
            "and status = :status "
            "and staged_for_transcription = false "
            "and clipped_utterance_duration >= 0.5 and clipped_utterance_duration <= 15",
            **parm_dict,
        )
        return data

    def update_utterances(self, audio_id, utterances):
        update_query = (
            "update media_metadata_staging "
            "set utterances_files_list = :utterances where audio_id = :audio_id"
        )
        utterances_json_str = json.dumps(utterances)
        LOGGER.info("utterances_json_str:%s", utterances_json_str)
        LOGGER.info("utterances:%s", str(utterances))
        parm_dict = {"utterances": utterances_json_str, "audio_id": audio_id}
        self.postgres_client.execute_update(update_query, **parm_dict)
        return True

    def find_utterance_by_name(self, utterances, name):
        filtered_utterances = list(filter(lambda d: d["name"] == name, utterances))
        if len(filtered_utterances) > 0:
            return filtered_utterances[0]
        else:
            return None

    def update_utterance_status(self, audio_id, utterance):
        update_query = (
            "update media_speaker_mapping set status = :status, "
            "fail_reason = :reason where audio_id = :audio_id "
            "and clipped_utterance_file_name = :name"
        )
        name = utterance["name"]
        reason = utterance["reason"]
        status = utterance["status"]
        param_dict = {
            "status": status,
            "reason": reason,
            "audio_id": audio_id,
            "name": name,
        }
        self.postgres_client.execute_update(update_query, **param_dict)
        return True

    def update_utterances_staged_for_transcription(self, utterances, source):
        if len(utterances) <= 0:
            return True

        update_query = (
            "update media_speaker_mapping set staged_for_transcription = true "
            "where audio_id in (select audio_id from media_metadata_staging "
            'where "source" = :source) and clipped_utterance_file_name in '
        )
        utterance_names = list(map(lambda u: f"'{u[1]}'", utterances))
        update_query = update_query + "(" + ",".join(utterance_names) + ")"
        param_dict = {"source": source}
        self.postgres_client.execute_update(update_query, **param_dict)
        return True

    def get_unique_id(self):
        return self.postgres_client.execute_query(GET_UNIQUE_ID)[0][0]

    def check_file_exist_in_db(self, file_name, hash_code):

        param_dict = {"file_name": file_name, "hash_code": hash_code}
        return self.postgres_client.execute_query(IS_EXIST, **param_dict)[0][0]

    def upload_file(self, meta_data_path):
        """
        Uploading the meta data file from local to
        """
        db = self.postgres_client.db

        with open(meta_data_path, "r") as file:
            dataframe = pd.read_csv(meta_data_path)
            columns = dataframe.columns
            cmd = COMMAND_WITHOUT_LICENSE
            if LICENSE in columns:
                cmd = COMMAND_WITH_LICENSE
            conn = db.raw_connection()
            cursor = conn.cursor()
            cursor.copy_expert(cmd, file)
            conn.commit()

    def upload_file_to_downloaded_source(self, file_path):

        db_conn = self.postgres_client.db

        LOGGER.info("uploading data to source_metadata")
        with open(file_path, "r") as file:
            conn = db_conn.raw_connection()
            cursor = conn.cursor()
            cmd = (
                "COPY source_metadata_downloaded(source,num_speaker,total_duration,num_of_audio)"
                " FROM STDIN WITH (FORMAT CSV, HEADER)"
            )
            cursor.copy_expert(cmd, file)
            conn.commit()

    def insert_speaker(self, source, speaker_name):
        param_dict = {"speaker_name": speaker_name, "source": source}
        insert_query = (
            "insert into speaker (source, speaker_name) values (:source, :speaker_name)"
        )
        self.postgres_client.execute_update(insert_query, **param_dict)
        return True

    def update_utterance_speaker(
        self, utterance_file_names, speaker_name, was_noise=False
    ):
        update_query = (
            "update media_speaker_mapping "
            "set speaker_id=(select speaker_id from speaker where speaker_name=:speaker_name "
            "limit 1) "
            ", was_noise=:was_noise "
            "where clipped_utterance_file_name in "
        )
        utterance_names = list(map(lambda u: f"'{u}'", utterance_file_names))
        update_query = update_query + "(" + ",".join(utterance_names) + ")"
        param_dict = {"speaker_name": speaker_name, "was_noise": was_noise}
        self.postgres_client.execute_update(update_query, **param_dict)
        return True

    def select_speaker(self, speaker_name, source):
        param_dict = {"speaker_name": speaker_name, "source": source}
        sql = "select speaker_id from speaker where speaker_name=:speaker_name and source=:source"
        result = self.postgres_client.execute_query(sql, **param_dict)
        return result[0][0] if len(result) > 0 else -1

    def update_utterance_speaker_gender(self, utterance_file_names, speaker_gender):
        update_query = (
            "update media_speaker_mapping "
            "set speaker_gender=:speaker_gender "
            " where clipped_utterance_file_name in "
        )

        utterance_names = list(map(lambda u: f"'{u}'", utterance_file_names))
        update_query = update_query + "(" + ",".join(utterance_names) + ")"

        param_dict = {"speaker_gender": speaker_gender}

        self.postgres_client.execute_update(update_query, **param_dict)
        return True

    def get_utterance_details_by_source(self, source, language):
        return []