import sys

from common.utils import get_logger

from audio_analysis.speaker_analysis.create_embeddings import encoder
from audio_analysis.speaker_analysis.file_cluster_mapping import speaker_to_file_name_map
from audio_analysis.speaker_analysis.speaker_clustering import create_speaker_clusters

sys.path.insert(0, '..')
sys.path.insert(0, '../..')

Logger = get_logger("analyse_speakers")


def analyse_speakers(embed_file_path, dir_pattern, local_audio_download_path, source, catalogue_dao,
                     min_cluster_size, partial_set_size, min_samples):
    encoder(local_audio_download_path, dir_pattern, embed_file_path)
    file_map_dict, noise_file_map_dict = create_speaker_clusters(embed_file_path, source, min_cluster_size, partial_set_size, min_samples)
    speaker_to_file_name = speaker_to_file_name_map(file_map_dict)
    Logger.info('total speakers:' + str(len(speaker_to_file_name)))
    for speaker in speaker_to_file_name:
        speaker_id = catalogue_dao.select_speaker(speaker, source)
        if speaker_id == -1:
            speaker_inserted = catalogue_dao.insert_speaker(source, speaker)
        else:
            Logger.info("Speaker already exists:" + speaker)
            speaker_inserted = True

        if speaker_inserted:
            Logger.info('updating utterances for speaker:' + speaker)
            Logger.info('utterances:' + str(speaker_to_file_name.get(speaker)))
            catalogue_dao.update_utterance_speaker(speaker_to_file_name.get(speaker), speaker)
