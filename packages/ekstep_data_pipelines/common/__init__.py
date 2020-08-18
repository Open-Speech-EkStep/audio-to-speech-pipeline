import yaml
from common.data_processor import DataProcessorUtil
from common.gcs_operations import CloudStorageOperations
from common.audio_commons import get_audio_commons

def get_periperhals(intialization_dict_path):
    data_processor = DataProcessorUtil.get_instance(intialization_dict_path)
    gcs_instance = CloudStorageOperations.get_instance(intialization_dict_path)

    peripheral_dict ={
        "data_processor": data_processor,
        "gsc_instance": gcs_instance
    }

    config_dict = load_config(intialization_dict_path)

    peripheral_dict['audio_commons'] = get_audio_commons(config_dict)

    return peripheral_dict


def load_config(config_file_path):
    with open(config_file_path, 'r') as file:
        parent_config_dict = yaml.load(file)
        return parent_config_dict.get('config')



