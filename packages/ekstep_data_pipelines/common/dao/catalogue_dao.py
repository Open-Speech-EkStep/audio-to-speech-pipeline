import json

from common.utils import get_logger


LOGGER = get_logger('CatalogueDao')

class CatalogueDao:

    def __init__(self, postgres_client):
        self.postgres_client = postgres_client

    def get_utterances(self, audio_id):
        parm_dict = {'audio_id': audio_id}
        utterances = self.postgres_client \
            .execute_query('select utterances_files_list from media_metadata_staging where audio_id = :audio_id'
                           , **parm_dict)
        return json.loads(utterances[0][0]) if len(utterances) > 0 else []

    def get_utterances_by_source(self, source, status):
        parm_dict = {'source': source, 'status': status}
        data = self.postgres_client \
            .execute_query('select speaker_id, clipped_utterance_file_name, clipped_utterance_duration, audio_id, snr '
                           'from media_speaker_mapping '
                           'where audio_id '
                           'in (select audio_id from media_metadata_staging where "source" = :source) '
                           'and status = :status '
                           'and staged_for_transcription = false '
                           'and clipped_utterance_duration >= 0.5 and clipped_utterance_duration <= 15'
                           , **parm_dict)
        return data

    def update_utterances(self, audio_id, utterances):
        update_query = 'update media_metadata_staging ' \
                       'set utterances_files_list = :utterances where audio_id = :audio_id'
        utterances_json_str = json.dumps(utterances)
        LOGGER.info('utterances_json_str:' + utterances_json_str)
        LOGGER.info('utterances:' + str(utterances))
        parm_dict = {'utterances': utterances_json_str, 'audio_id': audio_id}
        self.postgres_client.execute_update(update_query, **parm_dict)
        return True

    def find_utterance_by_name(self, utterances, name):
        filtered_utterances = list(filter(lambda d: d['name'] == name, utterances))
        if len(filtered_utterances) > 0:
            return filtered_utterances[0]
        else:
            return None

    def update_utterance_status(self, audio_id, utterance):
        update_query = 'update media_speaker_mapping set status = :status, ' \
                       'fail_reason = :reason where audio_id = :audio_id ' \
                       'and clipped_utterance_file_name = :name'
        name = utterance['name']
        reason = utterance['reason']
        status = utterance['status']
        param_dict = {'status': status, 'reason': reason, 'audio_id': audio_id, 'name': name}
        self.postgres_client.execute_update(update_query, **param_dict)
        return True

    def update_utterances_staged_for_transcription(self, utterances, source):
        if len(utterances) <= 0:
            return True

        update_query = 'update media_speaker_mapping set staged_for_transcription = true ' \
                       'where audio_id in (select audio_id from media_metadata_staging ' \
                       'where "source" = :source) and clipped_utterance_file_name in '
        utterance_names = list(map(lambda u: f'\'{u[1]}\'', utterances))
        update_query = update_query + '(' + ','.join(utterance_names) + ')'
        param_dict = {'source': source}
        self.postgres_client.execute_update(update_query, **param_dict)
        return True
