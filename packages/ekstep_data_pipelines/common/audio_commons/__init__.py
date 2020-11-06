from ekstep_data_pipelines.common.audio_commons.chunking_conversion_util import ChunkingConversionUtil
from ekstep_data_pipelines.common.audio_commons.snr_util import SNR
from ekstep_data_pipelines.common.audio_commons.transcription_clients import get_transcription_clients

def get_audio_commons(initlization_dict):
    audio_commons_dict = {}

    audio_commons_dict['chunking_conversion'] = ChunkingConversionUtil.get_instance()
    audio_commons_dict['snr_util'] = SNR.get_instance(initlization_dict)
    audio_commons_dict['transcription_clients'] = get_transcription_clients(initlization_dict)

    return audio_commons_dict
