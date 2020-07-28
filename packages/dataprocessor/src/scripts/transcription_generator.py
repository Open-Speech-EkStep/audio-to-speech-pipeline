from .extract_transcription import extract_transcription
from .save_transcription import save_transcription, save_transcriptions


def create_transcriptions(google_speech_client, wav_file_path, output_path, api_response_file):
    response = google_speech_client.call_speech_to_text(wav_file_path, True, output_path, api_response_file)
    transcriptions = extract_transcription(response)
    save_transcriptions(output_path, transcriptions, 'chunk')
    return transcriptions


def create_transcription(azure_client, language, wav_file_path):
    result = azure_client.speech_to_text(wav_file_path, language)
    transcription_file_path = wav_file_path.replace('.wav', '.txt')
    save_transcription(result.text, transcription_file_path)
