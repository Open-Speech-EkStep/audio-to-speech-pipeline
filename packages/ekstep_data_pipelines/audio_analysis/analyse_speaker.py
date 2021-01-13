
from ekstep_data_pipelines.common.utils import get_logger

from ekstep_data_pipelines.audio_analysis.speaker_analysis.create_embeddings import encoder
from ekstep_data_pipelines.audio_analysis.speaker_analysis.file_cluster_mapping import speaker_to_file_name_map
from ekstep_data_pipelines.audio_analysis.speaker_analysis.speaker_clustering import create_speaker_clusters


Logger = get_logger("analyse_speakers")


def analyse_speakers(embed_file_path, dir_pattern, local_audio_download_path, source, catalogue_dao,
                     min_cluster_size, partial_set_size, min_samples, fit_noise_on_similarity,
                     fs_interface, npz_bucket_destination_path):
    encoder(local_audio_download_path, dir_pattern, embed_file_path)
    is_uploaded = fs_interface.upload_to_location(embed_file_path, npz_bucket_destination_path)
    if is_uploaded:
        Logger.info('npz file uploaded to gcp :' + npz_bucket_destination_path)
    else:
        Logger.info('npz file could not be uploaded to :' + npz_bucket_destination_path)
    file_map_dict, noise_file_map_dict = create_speaker_clusters(embed_file_path, source, min_cluster_size, partial_set_size, min_samples, fit_noise_on_similarity)
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
            utterances = speaker_to_file_name.get(speaker)
            Logger.info('utterances:' + str(utterances))
            to_file_name = lambda u: u[0]
            was_noise_utterances = list(map(to_file_name, (filter(lambda u: u[1] == 1, utterances))))
            fitted_utterances = list(map(to_file_name, (filter(lambda u: u[1] == 0, utterances))))
            if len(was_noise_utterances) > 0:
                catalogue_dao.update_utterance_speaker(was_noise_utterances, speaker, True)
            if len(fitted_utterances) > 0:
                catalogue_dao.update_utterance_speaker(fitted_utterances, speaker, False)
