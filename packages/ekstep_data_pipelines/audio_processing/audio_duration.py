import librosa
import sox


def calculate_duration(input_filepath):
    return sox.file_info.duration(input_filepath)


def calculate_duration_librosa(input_filepath):
    y, sr = librosa.load(input_filepath)
    return librosa.get_duration(y)
