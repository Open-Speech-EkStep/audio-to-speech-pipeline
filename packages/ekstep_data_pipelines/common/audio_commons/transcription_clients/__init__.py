from ekstep_data_pipelines.common.audio_commons.transcription_clients.azure_transcription_client \
    import (
    AzureTranscriptionClient, )
from ekstep_data_pipelines.common.audio_commons.transcription_clients.google_transcription_client \
    import (
    GoogleTranscriptionClient, )


def get_transcription_clients(initialization_dict):
    azure_transcription_client = AzureTranscriptionClient.get_instance(
        initialization_dict
    )
    google_transcription_client = GoogleTranscriptionClient.get_instance(
        initialization_dict
    )

    return {"azure": azure_transcription_client,
            "google": google_transcription_client}
