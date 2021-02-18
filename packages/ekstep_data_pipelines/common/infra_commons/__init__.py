from ekstep_data_pipelines.common.infra_commons.storage import get_storage_clients


def get_infra_utils(intialization_dict):
    infra_util_dict = {}
    storage_clients = get_storage_clients(intialization_dict)
    infra_util_dict["storage_clients"] = storage_clients
    return infra_util_dict
