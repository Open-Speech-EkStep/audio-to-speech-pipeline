import argparse
import json
import os
import uuid

from ekstep_data_pipelines.audio_analysis.audio_analysis import AudioAnalysis
from ekstep_data_pipelines.audio_cataloguer.cataloguer import AudioCataloguer
from ekstep_data_pipelines.audio_embedding.audio_embedding import AudioEmbedding
from ekstep_data_pipelines.audio_processing.audio_processer import AudioProcessor
from ekstep_data_pipelines.audio_transcription.audio_transcription import (
    AudioTranscription,
)
from ekstep_data_pipelines.common import get_periperhals
from ekstep_data_pipelines.common.utils import get_logger
from ekstep_data_pipelines.data_marker.data_marker import DataMarker
from ekstep_data_pipelines.ulca.ulca_dataset import ULCADataset
from google.cloud import storage

STT_CLIENT = ["google", "azure", "ekstep"]


class ACTIONS:
    DATA_MARKING = "data_marking"
    AUDIO_PROCESSING = "audio_processing"
    AUDIO_TRANSCRIPTION = "audio_transcription"
    AUDIO_ANALYSIS = "audio_analysis"
    AUDIO_CATALOGUER = "audio_cataloguer"
    AUDIO_EMBEDDING = "audio_embedding"
    ULCA_DATASET = "ulca_dataset"


class FileSystems:
    GOOGLE = "google"
    LOCAL = "local"


LOGGER = get_logger("EKSTEP_PROCESSOR")
ACTIONS_LIST = [
    ACTIONS.DATA_MARKING,
    ACTIONS.AUDIO_PROCESSING,
    ACTIONS.AUDIO_TRANSCRIPTION,
    ACTIONS.AUDIO_ANALYSIS,
    ACTIONS.AUDIO_CATALOGUER,
    ACTIONS.AUDIO_EMBEDDING,
    ACTIONS.ULCA_DATASET
]
FILES_SYSTEMS_LIST = [FileSystems.GOOGLE, FileSystems.LOCAL]

parser = argparse.ArgumentParser(description="Util for data processing for EkStep")

parser.add_argument(
    "-b",
    "--bucket",
    dest="config_bucket",
    default="ekstepspeechrecognition-dev",
    help="Bucket name as per the environment",
)

parser.add_argument(
    "-a",
    "--action",
    dest="action",
    default=None,
    choices=ACTIONS_LIST,
    required=True,
    help="Action for the processor to perform",
)

parser.add_argument(
    "-c",
    "--config-path",
    dest="local_config",
    default=None,
    help="path to local config, use this when running on local",
)

parser.add_argument(
    "-rc",
    "--remote-config-path",
    dest="remote_config",
    default=None,
    help="path to remote gcs config file. Use this when running on cluster mode",
)

parser.add_argument(
    "-fl",
    "--filename-list",
    dest="file_name_list",
    default=[],
    help="list of all the filename that need to processed, this needs to a comma seperated "
         "list eg. audio_id1,audio_id2 . Only works with the audio processor",
)

parser.add_argument(
    "-ai",
    "--audio-ids",
    dest="audio_ids",
    default=[],
    help="list of all the audio ids that need to processed, this needs to a comma seperated "
         "list eg. audio_id1,audio_id2 . Only works with the audio processor",
)

parser.add_argument(
    "-as",
    "--audio-source",
    dest="audio_source",
    default=None,
    help="The name of the source of the audio which is being processed. Only works with "
         "audio processor",
)

parser.add_argument(
    "-af",
    "--audio-format",
    dest="audio_format",
    default=None,
    help="The format of the audio which is being processed eg mp4,mp3 . Only works with "
         "audio processor",
)

parser.add_argument(
    "-stt",
    "--speech-to-text",
    dest="speech_to_text_client",
    default=None,
    help="The client name which we want to call for stt",
)

parser.add_argument(
    "-fs",
    "--filter_spec",
    dest="filter_spec",
    default=None,
    help="The filter spec that needs to be applied for data marking",
)

parser.add_argument(
    "-par",
    "--params",
    dest="params",
    default=None,
    help="The parameters that need to be used in speaker clustering",
)

parser.add_argument(
    "-fp",
    "--file-path",
    dest="file_path",
    default=None,
    help="The parameters that need to be used in audio embedding and data marking",
)

parser.add_argument(
    "-sp",
    "--source-path-stt",
    dest="source_path_stt",
    default='dummy',
    help="The parameters that need to be used in audio stt for non default path",
)

parser.add_argument(
    "-ds",
    "--data-set",
    dest="data_set",
    default=None,
    help="The dataset type that need to be used in audio embedding",
)
parser.add_argument(
    "-f",
    "--file_system",
    dest="file_system",
    choices=FILES_SYSTEMS_LIST,
    default="google",
    help="Specify the file system to use for running the pipeline",
    required=False,
)

parser.add_argument(
    "-ulca_config",
    "--ulca_config",
    dest="ulca_config",
    help="Specify ulca config",
    required=False,
)

parser.add_argument(
    "-l", "--language", dest="language", default="hindi", help="Specify the language"
)

processor_args = parser.parse_args()


def download_config_file(config_file_path, config_bucket):
    LOGGER.info("Downloading config file from Google Cloud Storage")

    download_file_path = f"/tmp/{str(uuid.uuid4())}"
    gcs_storage_client = storage.Client()
    bucket = gcs_storage_client.bucket(config_bucket)

    LOGGER.info("Getting config file from config bucket %s", config_bucket)

    src_blob = bucket.blob(config_file_path)
    src_blob.download_to_filename(download_file_path)

    LOGGER.info("Config file downloaded to %s", download_file_path)

    return download_file_path


def process_config_input(arguments):
    LOGGER.info("validating config file path")

    LOGGER.info("Checking configeration file path")
    config_file_path = None

    if arguments.local_config is None and arguments.remote_config is None:
        raise argparse.ArgumentTypeError("No config specified")

    if arguments.local_config is not None and arguments.remote_config is not None:
        raise argparse.ArgumentTypeError(
            "mulitple configs specified, specify only local_config or remote_config but not both"
        )

    if arguments.local_config:
        LOGGER.info("Checking the file path on local machine")
        config_file_path = arguments.local_config
        exists = os.path.exists(config_file_path)

        if not exists:
            raise argparse.ArgumentTypeError(
                f"Cannot find config file on path {config_file_path}"
            )

    if arguments.remote_config:
        LOGGER.info(
            "http/https file path %s found for config file. " "Downloading config file",
            arguments.remote_config,
        )
        config_file_path = download_config_file(
            arguments.remote_config, arguments.config_bucket
        )

    return config_file_path


def check_if_csv_file_path_valid(file_path):
    try:
        file_ext = file_path.split('/')[-1].split('.')[-1]
        if file_ext == 'csv':
            return True
    except Exception as e:
        return False


def validate_data_filter_config(arguments):
    LOGGER.info("validating input for audio processing")

    if arguments.audio_source is None:
        raise argparse.ArgumentTypeError("Source is missing")

    if arguments.filter_spec is None:
        raise argparse.ArgumentTypeError("Filter spec is missing")

    if arguments.filter_spec is not None:
        filter_spec_dict = json.loads(arguments.filter_spec)
        if filter_spec_dict.get("language", None) is None:
            raise argparse.ArgumentTypeError("Language for the source is not specified")
        if filter_spec_dict.get("data_set", None) not in ('train', 'test'):
            raise argparse.ArgumentTypeError("Filter spec has no proper data set type")
        if filter_spec_dict.get("file_mode", None) in (None, 'n', 'N') and filter_spec_dict.get("filter", None) is None:
            raise argparse.ArgumentTypeError("Filter spec has no file_mode and no filter")
        if filter_spec_dict.get("file_mode", None) not in ('y', 'n', 'Y', 'N', None):
            raise argparse.ArgumentTypeError("Filter spec has no proper file_mode")
        if filter_spec_dict.get("file_mode", None) in ('y', 'Y') and filter_spec_dict.get("file_path", None) is None:
            raise argparse.ArgumentTypeError("Filter spec has file_mode but no file path")
        if filter_spec_dict.get("file_path", None) is not None and not check_if_csv_file_path_valid(
                filter_spec_dict.get("file_path", None)):
            raise argparse.ArgumentTypeError("Filter spec has no valid csv file path")

    return {"filter_spec": filter_spec_dict,
            "source": arguments.audio_source}


def validate_ulca_dataset_config(arguments):
    LOGGER.info("validating input for ulca dataset")

    if arguments.audio_source is None:
        raise argparse.ArgumentTypeError("Source is missing")

    if arguments.ulca_config is None:
        raise argparse.ArgumentTypeError("ulca_config is missing")

    return {"source": arguments.audio_source, "ulca_config": arguments.ulca_config}


def validate_audio_analysis_config(arguments):
    LOGGER.info("validating input for audio analysis")

    if arguments.audio_source is None:
        raise argparse.ArgumentTypeError("Source is missing")
    return {"source": arguments.audio_source}


def validate_audio_embedding_config(arguments):
    LOGGER.info("validating input for audio embedding")

    if arguments.file_path is None:
        raise argparse.ArgumentTypeError(f"file path is missing")
    return {"file_path": arguments.file_path}


def validate_audio_processing_input(arguments):
    LOGGER.info("validating input for audio processing")

    if arguments.file_name_list == []:
        raise argparse.ArgumentTypeError(
            "Audio Id list missing. Please specify comma seperated audio IDs for processing"
        )

    file_name_list = [
        i.strip() for i in list(filter(None, arguments.file_name_list.split(",")))
    ]

    if file_name_list == []:
        raise argparse.ArgumentTypeError(
            "Audio Id list missing. Please specify comma seperated audio IDs for processing"
        )

    if arguments.audio_source is None:
        raise argparse.ArgumentTypeError(
            "Audio Source missing. Please specify source for the source for the audio"
        )

    audio_source = arguments.audio_source

    if arguments.audio_format is None:
        raise argparse.ArgumentTypeError(
            "Audio format missing. Please specify formar for the audio"
        )

    audio_format = arguments.audio_format

    return {
        "file_name_list": file_name_list,
        "source": audio_source,
        "extension": audio_format,
    }


def validate_audio_transcription_input(arguments):
    if arguments.audio_ids == []:
        raise argparse.ArgumentTypeError(
            "Audio Id list missing. Please audio ID for processing"
        )

    audio_ids = [i.strip() for i in list(filter(None, arguments.audio_ids.split(",")))]

    if arguments.speech_to_text_client not in STT_CLIENT:
        raise argparse.ArgumentTypeError("Stt client must be google or azure or ekstep")

    speech_to_text_client = arguments.speech_to_text_client

    if arguments.audio_source is None:
        raise argparse.ArgumentTypeError(
            "Audio Source missing. Please specify source for the source for the audio"
        )

    audio_source = arguments.audio_source

    if arguments.data_set is None or (arguments.data_set not in ('train', 'test', '')):
        raise argparse.ArgumentTypeError(f"data set type is missing or incorrect")

    data_set = arguments.data_set

    LOGGER.info(f"Source path is {arguments.source_path_stt}")
    return {
        "audio_ids": audio_ids,
        "speech_to_text_client": speech_to_text_client,
        "audio_source": audio_source,
        "data_set": data_set,
        "language": arguments.language,
        "source_path_stt": arguments.source_path_stt
    }


def perform_action(arguments, **kwargs):
    current_action = arguments.action

    curr_processor = None

    if current_action == ACTIONS.DATA_MARKING:
        kwargs.update(validate_data_filter_config(arguments))
        LOGGER.info("Intializing data marker with given config")

        config_params = {"config_file_path": kwargs.get("config_file_path")}

        object_dict = get_periperhals(config_params, arguments.language)

        data_processor = object_dict.get("data_processor")
        gcs_instance = object_dict.get("gsc_instance")

        curr_processor = DataMarker.get_instance(
            data_processor,
            gcs_instance,
            **{"commons_dict": object_dict, "file_interface": arguments.file_system},
        )

    elif current_action == ACTIONS.AUDIO_PROCESSING:
        kwargs.update(validate_audio_processing_input(arguments))
        LOGGER.info("Intializing audio processor marker with given config")
        config_params = {"config_file_path": kwargs.get("config_file_path")}

        object_dict = get_periperhals(config_params, arguments.language)

        data_processor = object_dict.get("data_processor")
        gcs_instance = object_dict.get("gsc_instance")
        audio_commons = object_dict.get("audio_commons")
        catalogue_dao = object_dict.get("catalogue_dao")

        curr_processor = AudioProcessor.get_instance(
            data_processor,
            gcs_instance,
            audio_commons,
            catalogue_dao,
            **{"commons_dict": object_dict, "file_interface": arguments.file_system},
        )

    elif current_action == ACTIONS.AUDIO_TRANSCRIPTION:
        kwargs.update(validate_audio_transcription_input(arguments))
        LOGGER.info("Intializing audio processor marker with given config")
        config_params = {"config_file_path": kwargs.get("config_file_path")}

        object_dict = get_periperhals(config_params, arguments.language)

        data_processor = object_dict.get("data_processor")
        gcs_instance = object_dict.get("gsc_instance")
        audio_commons = object_dict.get("audio_commons")
        catalogue_dao = object_dict.get("catalogue_dao")

        curr_processor = AudioTranscription.get_instance(
            data_processor,
            gcs_instance,
            audio_commons,
            catalogue_dao,
            **{"commons_dict": object_dict, "file_interface": arguments.file_system},
        )
    elif current_action == ACTIONS.AUDIO_ANALYSIS:
        kwargs.update(validate_audio_analysis_config(arguments))
        LOGGER.info("Intializing audio analysis processor with given config")

        config_params = {"config_file_path": kwargs.get("config_file_path")}

        object_dict = get_periperhals(config_params, arguments.language)

        data_processor = object_dict.get("data_processor")

        curr_processor = AudioAnalysis.get_instance(
            data_processor,
            **{"commons_dict": object_dict, "file_interface": arguments.file_system},
        )
        LOGGER.info("Starting processing for %s", current_action)

    elif current_action == ACTIONS.AUDIO_CATALOGUER:
        LOGGER.info("Intializing data AudioCataloguer with given config")

        config_params = {"config_file_path": kwargs.get("config_file_path")}

        object_dict = get_periperhals(config_params, arguments.language)

        data_processor = object_dict.get("data_processor")

        curr_processor = AudioCataloguer.get_instance(data_processor)
        LOGGER.info("Starting processing for %s", current_action)
        LOGGER.info(f"Starting processing for {current_action}")

    elif current_action == ACTIONS.AUDIO_EMBEDDING:

        kwargs.update(validate_audio_embedding_config(arguments))

        LOGGER.info("Intializing data AudioEmbedding with given config")

        config_params = {"config_file_path": kwargs.get("config_file_path")}

        object_dict = get_periperhals(config_params, arguments.language)

        data_processor = object_dict.get("data_processor")

        curr_processor = AudioEmbedding.get_instance(data_processor, **{"commons_dict": object_dict,
                                                                        "file_interface": arguments.file_system})
        LOGGER.info(f"Starting processing for {current_action}")

    elif current_action == ACTIONS.ULCA_DATASET:
        kwargs.update(validate_ulca_dataset_config(arguments))
        LOGGER.info("Intializing ulca dataset process with given config")

        config_params = {"config_file_path": kwargs.get("config_file_path")}

        object_dict = get_periperhals(config_params, arguments.language)

        data_processor = object_dict.get("data_processor")

        curr_processor = ULCADataset.get_instance(
            data_processor,
            **{"commons_dict": object_dict, "file_interface": arguments.file_system},
        )

    curr_processor.process(**kwargs)
    LOGGER.info("Ending processing for %s", current_action)


if __name__ == "__main__":
    config_file_path_ = process_config_input(processor_args)
    LOGGER.info("Loaded configuration file path, performing action")
    action_kwargs = {"config_file_path": config_file_path_}
    perform_action(processor_args, **action_kwargs)
