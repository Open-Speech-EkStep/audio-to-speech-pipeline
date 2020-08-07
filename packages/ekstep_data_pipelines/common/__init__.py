from common.data_processor import DataProcessorUtil
from common.gcs_operations import CloudStorageOperations

def get_periperhals(intialization_dict):
    data_processor = DataProcessorUtil.get_instance(intialization_dict)
    gcs_instance = CloudStorageOperations.get_instance()

    peripheral_dict ={
        "data_processor": data_processor,
        "gsc_instance": gcs_instance
    }

    return peripheral_dict
