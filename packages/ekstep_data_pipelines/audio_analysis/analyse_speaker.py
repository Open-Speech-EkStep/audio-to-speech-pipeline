import sys

from common.utils import get_logger

from audio_analysis.speaker_analysis.create_embeddings import encoder
from audio_analysis.speaker_analysis.file_cluster_mapping import speaker_to_file_name_map
from audio_analysis.speaker_analysis.speaker_clustering import create_speaker_clusters

sys.path.insert(0, '..')
sys.path.insert(0, '../..')

Logger = get_logger("analyse_speakers")


def analyse_speakers(embed_file_path, dir_pattern, local_audio_download_path, source, catalogue_dao):
    encoder(local_audio_download_path, dir_pattern, embed_file_path)
    file_map_dict, noise_file_map_dict = create_speaker_clusters(embed_file_path, source)
    speaker_to_file_name = speaker_to_file_name_map(file_map_dict)
    Logger.info('utterance name to speaker mapping:' + str(speaker_to_file_name))
    for speaker in speaker_to_file_name:
        speaker_inserted = catalogue_dao.insert_speaker(source, speaker)
        if speaker_inserted:
            catalogue_dao.update_utterance_speaker(speaker_to_file_name.get(speaker), speaker)
            Logger.info('updating utterances for speaker:'+ speaker)