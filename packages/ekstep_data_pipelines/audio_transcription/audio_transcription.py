import traceback

from audio_transcription.constants import CONFIG_NAME, CLEAN_AUDIO_PATH, LANGUAGE
from audio_transcription.transcription_sanitizer import TranscriptionSanitizer
from audio_transcription.audio_transcription_errors import TranscriptionSanitizationError
from common.audio_commons.transcription_clients.transcription_client_errors import \
    AzureTranscriptionClientError, GoogleTranscriptionClientError
from common.file_utils import get_file_name
from common.utils import get_logger

import os

from common.dao.catalogue_dao import CatalogueDao

LOGGER = get_logger('audio_transcription')


class AudioTranscription:
    LOCAL_PATH = None

    @staticmethod
    def get_instance(data_processor, gcs_instance, audio_commons, catalogue_dao):
        return AudioTranscription(data_processor, gcs_instance, audio_commons, catalogue_dao)

    def __init__(self, data_processor, gcs_instance, audio_commons, catalogue_dao):
        self.data_processor = data_processor
        self.gcs_instance = gcs_instance
        self.transcription_clients = audio_commons.get('transcription_clients')
        self.catalogue_dao = catalogue_dao
        self.audio_transcription_config = None

    def process(self, **kwargs):

        self.audio_transcription_config = self.data_processor.config_dict.get(
            CONFIG_NAME)

        source = kwargs.get('audio_source')
        audio_ids = kwargs.get('audio_ids', [])
        stt_api = kwargs.get("speech_to_text_client")

        language = self.audio_transcription_config.get(LANGUAGE)
        remote_path_of_dir = self.audio_transcription_config.get(
            CLEAN_AUDIO_PATH)
        LOGGER.info('Generating transcriptions for audio_ids:' + str(audio_ids))
        failed_audio_ids = []
        for audio_id in audio_ids:
            try:
                LOGGER.info('Generating transcription for audio_id:' + str(audio_id))
                utterances = self.catalogue_dao.get_utterances(audio_id)
                if len(utterances) <= 0:
                    LOGGER.info('No utterances found for audio_id:' + audio_id)
                    continue
                LOGGER.info("before transcription utterances:" + str(utterances))
                remote_dir_path_for_given_audio_id = f'{remote_path_of_dir}/{source}/{audio_id}/clean/'
                remote_stt_output_path = self.audio_transcription_config.get(
                    'remote_stt_audio_file_path')
                remote_stt_output_path = f'{remote_stt_output_path}/{source}/{audio_id}'

                transcription_client = self.transcription_clients[stt_api]
                LOGGER.info('Using transcription client:' + str(transcription_client))
                all_path = self.gcs_instance.list_blobs_in_a_path(remote_dir_path_for_given_audio_id)

                local_clean_dir_path, local_rejected_dir_path = self.generate_transcription_for_all_utterenaces(audio_id, all_path, language,
                                                                                 transcription_client, utterances)
                LOGGER.info("after transcription utterances:" + str(utterances))
                LOGGER.info('updating catalogue with updated utterances')
                self.catalogue_dao.update_utterances(audio_id, utterances)

                LOGGER.info(f'Uploading local generated files from {local_clean_dir_path} to {remote_stt_output_path}')
                self.move_to_gcs(local_clean_dir_path, remote_stt_output_path + "/clean")

                LOGGER.info(f'Uploading local generated files from {local_rejected_dir_path} to {remote_stt_output_path}')
                if os.path.exists(local_rejected_dir_path):
                    self.move_to_gcs(local_rejected_dir_path, remote_stt_output_path + "/rejected")
                else:
                    LOGGER.info('No rejected files found')

                self.delete_audio_id(f'{remote_path_of_dir}/{source}/{audio_id}')
            except Exception as e:
                # TODO: This should be a specific exception, will need
                #       to throw and handle this accordingly.
                LOGGER.error(f'Transcription failed for audio_id:${audio_id}')
                LOGGER.error(str(e))
                traceback.print_exc()
                failed_audio_ids.append(audio_id)
                continue

        if len(failed_audio_ids) > 0:
            LOGGER.error('******* Job failed for one or more audio_ids')
            raise RuntimeError('Failed audio_ids:' + str(failed_audio_ids))
        return

    def delete_audio_id(self, remote_dir_path_for_given_audio_id):
        self.gcs_instance.delete_object(remote_dir_path_for_given_audio_id)

    def move_to_gcs(self, local_path, remote_stt_output_path):
        self.gcs_instance.upload_to_gcs(local_path, remote_stt_output_path)

    def save_transcription(self, transcription, output_file_path):
        with open(output_file_path, "w") as f:
            f.write(transcription)

    def generate_transcription_for_all_utterenaces(self, audio_id, all_path, language, transcription_client, utterances):
        LOGGER.info("*** generate_transcription_for_all_utterenaces **")
        local_clean_path = ''
        local_rejected_path = ''
        for file_path in all_path:
            file_name = get_file_name(file_path.name)
            local_clean_path = f"/tmp/{file_path.name}"
            local_rejected_path = local_clean_path.replace('clean', 'rejected')
            utterance_metadata = self.catalogue_dao.find_utterance_by_name(utterances, file_name)
            if utterance_metadata is None:
                LOGGER.info('No utterance found for file_name: ' + file_name)
                continue
            if utterance_metadata['status'] == 'Rejected':
                LOGGER.info('Skipping rejected file_name: ' + file_name)
                continue
            if float(utterance_metadata['duration']) < 0.5 or float(utterance_metadata['duration']) > 15 :
                LOGGER.error('skipping audio file as duration > 15 or < .5')
                continue

            LOGGER.info('Generating transcription for utterance:' + str(utterance_metadata))

            self.generate_transcription_and_sanitize(audio_id, local_clean_path, local_rejected_path, file_path, language,
                                                     transcription_client, utterance_metadata)

        return self.get_local_dir_path(local_clean_path), self.get_local_dir_path(local_rejected_path)

    def generate_transcription_and_sanitize(self, audio_id, local_clean_path, local_rejected_path, file_path, language,
                                            transcription_client, utterance_metadata):
        if ".wav" in file_path.name:

            transcription_file_name = local_clean_path.replace('.wav', '.txt')
            self.gcs_instance.download_to_local(
                file_path.name, local_clean_path, False)
            try:
                transcript = transcription_client.generate_transcription(
                    language, local_clean_path)
                original_transcript = transcript
                transcript = TranscriptionSanitizer().sanitize(transcript)

                if original_transcript != transcript:
                    old_file_name = get_file_name(transcription_file_name)
                    new_file_name = 'original_' + get_file_name(transcription_file_name)
                    file_name_with_original_prefix = transcription_file_name.replace(old_file_name, new_file_name)
                    LOGGER.info("saving original transcription to:" + file_name_with_original_prefix)
                    self.save_transcription(original_transcript, file_name_with_original_prefix)

                self.save_transcription(transcript, transcription_file_name)
            except TranscriptionSanitizationError as tse:
                LOGGER.error('Transcription not valid: ' + str(tse))
                reason = 'sanitization error:' + str(tse.args)
                self.handle_error(audio_id, local_clean_path, local_rejected_path, utterance_metadata, reason)
            except (AzureTranscriptionClientError, GoogleTranscriptionClientError) as e:
                LOGGER.error('STT API call failed: ' + str(e))
                reason = 'STT API error:' + str(e.args)
                self.handle_error(audio_id, local_clean_path, local_rejected_path, utterance_metadata, reason)
            except Exception as ex:
                LOGGER.error('Error: ' + str(ex))
                reason = ex.args
                self.handle_error(audio_id, local_clean_path, local_rejected_path, utterance_metadata, reason)

    def handle_error(self, audio_id, local_clean_path, local_rejected_path, utterance_metadata, reason):
        utterance_metadata['status'] = 'Rejected'
        utterance_metadata['reason'] = reason
        self.catalogue_dao.update_utterance_status(audio_id, utterance_metadata)
        rejected_dir = self.get_local_dir_path(local_rejected_path)
        if not os.path.exists(rejected_dir):
            os.makedirs(rejected_dir)
        command = f'mv {local_clean_path} {local_rejected_path}'
        LOGGER.info(f'moving bad wav file: {local_clean_path} to rejected folder: {local_rejected_path}')
        os.system(command)

    def get_local_dir_path(self, local_file_path):
        path_array = local_file_path.split('/')
        path_array.pop()
        return '/'.join(path_array)
