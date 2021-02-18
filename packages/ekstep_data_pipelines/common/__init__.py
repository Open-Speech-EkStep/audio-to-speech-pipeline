import yaml
import json
from ekstep_data_pipelines.common.postgres_db_client import PostgresClient
from ekstep_data_pipelines.common.gcs_operations import CloudStorageOperations
from ekstep_data_pipelines.common.audio_commons import get_audio_commons
from ekstep_data_pipelines.common.infra_commons import get_infra_utils
from ekstep_data_pipelines.common.dao.catalogue_dao import CatalogueDao


class BaseProcessor:
    def __init__(self, *agrs, **kwargs):
        self.commons_dict = kwargs.get("commons_dict", {})

        file_system = kwargs.get("file_interface")
        self.fs_interface = (
            self.commons_dict.get("infra_commons", {})
            .get("storage_clients", {})
            .get(file_system)
        )


def get_periperhals(intialization_dict_path, language):
    config_dict = load_config(intialization_dict_path.get("config_file_path"))

    config_dict_with_language = get_config_for_given_language(config_dict, language)

    data_processor = PostgresClient.get_instance(config_dict_with_language)
    gcs_instance = CloudStorageOperations.get_instance(config_dict_with_language)
    catalogue_dao = CatalogueDao(data_processor)

    peripheral_dict = {
        "data_processor": data_processor,
        "gsc_instance": gcs_instance,
        "catalogue_dao": catalogue_dao,
    }

    peripheral_dict["audio_commons"] = get_audio_commons(config_dict_with_language)
    peripheral_dict["infra_commons"] = get_infra_utils(config_dict_with_language)
    return peripheral_dict


def load_config(config_file_path):
    with open(config_file_path, "r") as file:
        parent_config_dict = yaml.load(file)
        return parent_config_dict.get("config")


def get_config_for_given_language(config_dict, language):
    dict_string = json.dumps(config_dict)
    replaced_string = dict_string.replace("{language}", language)
    config_dict = json.loads(replaced_string)
    return config_dict
