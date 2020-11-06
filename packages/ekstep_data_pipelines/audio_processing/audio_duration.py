import librosa
import sox

from ekstep_data_pipelines.common.utils import get_logger

LOGGER = get_logger('audio_duration')


def calculate_duration(input_filepath):
    duration = sox.file_info.duration(input_filepath)
    LOGGER.info(f'Duration for input_filepath:{input_filepath} : {str(duration)}')
    return duration


def calculate_duration_librosa(input_filepath):
    y, sr = librosa.load(input_filepath)
    return librosa.get_duration(y)
