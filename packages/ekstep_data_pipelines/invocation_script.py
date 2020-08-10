import os
import uuid
import argparse
from google.cloud import storage
from urllib.parse import urlparse
from data_marker.data_marker import DataMarker
from common.utils import get_logger
from common import get_periperhals


class ACTIONS:
    DATA_MARKING = 'data_marking'


LOGGER = get_logger('EKSTEP_PROCESSOR')
ACTIONS_LIST = [ACTIONS.DATA_MARKING]
CONFIG_BUCKET = 'ekstepspeechrecognition-dev'

parser = argparse.ArgumentParser(
    description='Util for data processing for EkStep')


parser.add_argument('-a', '--action', dest='action', default=None, choices=ACTIONS_LIST, required=True,
                    help='Action for the processor to perform')

parser.add_argument('-c', '--config-path', dest='local_config', default=None,
                    help='path to local config, use this when running on local')

parser.add_argument('-rc', '--remote-config-path', dest='remote_config', default=None,
                    help='path to remote gcs config file. Use this when running on cluster mode')


processor_args = parser.parse_args()


def download_config_file(config_file_path):
    LOGGER.info(f'Downloading config file from Google Cloud Storage')

    download_file_path = f'/tmp/{str(uuid.uuid4())}'
    gcs_storage_client = storage.Client()
    bucket = gcs_storage_client.bucket(CONFIG_BUCKET)

    LOGGER.info(f'Getting config file from config bucket {CONFIG_BUCKET}')

    src_blob = bucket.blob(config_file_path)
    src_blob.download_to_filename(download_file_path)

    LOGGER.info(f'Config file downloaded to {download_file_path}')

    return download_file_path


def process_config_input(arguments):
    LOGGER.info('validating config file path')

    LOGGER.info('Checking configeration file path')
    config_file_path = None

    if arguments.local_config == None and arguments.remote_config == None:
        raise argparse.ArgumentTypeError(f'No config specified')

    if arguments.local_config != None and arguments.remote_config != None:
        raise argparse.ArgumentTypeError(
            f'mulitple configs specified, specify only local_config or remote_config but not both')

    if arguments.local_config:
        LOGGER.info('Checking the file path on local machine')
        config_file_path = arguments.local_config
        exists = os.path.exists(config_file_path)

        if not exists:
            raise argparse.ArgumentTypeError(
                f'Cannot find config file on path {config_file_path}')

    if arguments.remote_config:
        LOGGER.info(
            f'http/https file path f{arguments.remote_config} found for config file. Downloading config file')
        config_file_path = download_config_file(arguments.remote_config)

    return config_file_path


def perform_action(arguments, **kwargs):
    current_action = arguments.action

    curr_processor = None

    if current_action == ACTIONS.DATA_MARKING:
        LOGGER.info('Intializing data marker with given config')

        config_params = {'config_file_path': kwargs.get('config_file_path')}

        object_dict = get_periperhals(config_params)

        data_processor = object_dict.get('data_processor')
        gcs_instance = object_dict.get('gsc_instance')

        curr_processor = DataMarker.get_instance(data_processor, gcs_instance)

    LOGGER.info(f'Starting processing for {current_action}')
    curr_processor.process()
    LOGGER.info(f'Ending processing for {current_action}')


if __name__ == "__main__":
    config_file_path = process_config_input(processor_args)
    LOGGER.info('Loaded configeration file path, performing action')
    action_kwargs = {'config_file_path': config_file_path}
    perform_action(processor_args, **action_kwargs)
