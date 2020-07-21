import sys
sys.path.append('./transcription')

from .extract_transcription import extract_transcription, save_transcriptions


def create_transcription(google_speech_client, wav_file_path, output_path, api_response_file):
    response = google_speech_client.call_speech_to_text(wav_file_path, True, output_path, api_response_file)
    transcriptions = extract_transcription(response)
    save_transcriptions(output_path, transcriptions, 'chunk')
    return transcriptions