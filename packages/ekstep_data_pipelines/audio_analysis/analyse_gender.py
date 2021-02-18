from ekstep_data_pipelines.common.utils import get_logger

from ekstep_data_pipelines.audio_analysis.audio_embeddings.gender_inference import (
    load_model,
    get_prediction_from_npz_file,
)


Logger = get_logger("analyse_speakers")


def analyse_gender(embed_file_path):
    Logger.info("Start analyse gender")
    gender_model = load_model("ekstep_data_pipelines/audio_analysis/models/clf_svc.sav")
    file_to_speaker_gender_mapping = get_prediction_from_npz_file(
        gender_model, embed_file_path
    )
    return file_to_speaker_gender_mapping
