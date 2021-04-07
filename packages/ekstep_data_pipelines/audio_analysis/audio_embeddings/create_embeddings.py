from ekstep_data_pipelines.common.utils import get_logger
from ekstep_data_pipelines.audio_analysis.speaker_analysis.speaker_clustering import (
    create_speaker_clusters,
)
from ekstep_data_pipelines.audio_analysis.speaker_analysis.file_cluster_mapping import (
    speaker_to_file_name_map,
)

Logger = get_logger("AudioSpeakerClusteringProcessor")


def create_embeddings(
    embed_file_path,
    fs_interface,
    npz_bucket_destination_path,
    source_name,
):
    is_uploaded = fs_interface.upload_to_location(
        embed_file_path, npz_bucket_destination_path
    )
    if is_uploaded:
        Logger.info("npz file uploaded to :%s", npz_bucket_destination_path)
    else:
        Logger.info(
            "npz file could not be uploaded to :%s", npz_bucket_destination_path
        )
    file_map_dict = create_speaker_clusters(embed_file_path, source_name)
    speaker_to_file_name = speaker_to_file_name_map(file_map_dict)
    Logger.info("total speakers:%s", str(len(speaker_to_file_name)))
