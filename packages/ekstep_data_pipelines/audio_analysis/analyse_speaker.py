from ekstep_data_pipelines.common.utils import get_logger


from ekstep_data_pipelines.audio_analysis.speaker_analysis.file_cluster_mapping import (
    speaker_to_file_name_map,
)
from ekstep_data_pipelines.audio_analysis.speaker_analysis.speaker_clustering import (
    create_speaker_clusters,
)
from ekstep_data_pipelines.common.utils import get_logger

Logger = get_logger("analyse_speakers")


def analyse_speakers(
    embed_file_path,
    source,
    min_cluster_size,
    partial_set_size,
    min_samples,
    fit_noise_on_similarity,
):
    file_map_dict, noise_file_map_dict = create_speaker_clusters(
        embed_file_path,
        source,
        min_cluster_size,
        partial_set_size,
        min_samples,
        fit_noise_on_similarity,
    )
    Logger.info("Noise count:", len(noise_file_map_dict))
    speaker_to_file_name = speaker_to_file_name_map(file_map_dict)
    Logger.info("total speakers:%s", str(len(speaker_to_file_name)))
    return speaker_to_file_name
