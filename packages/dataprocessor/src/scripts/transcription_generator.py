import sys
sys.path.append('./transcription')

from .extract_transcription import extract_transcription, save_transcriptions


def create_transcription(google_speech_client, wav_file_path, output_path):
    response = google_speech_client.call_speech_to_text(wav_file_path)
    transcriptions = extract_transcription(response)
    rejected_clips = save_transcriptions(output_path, transcriptions, 'chunk')
    return rejected_clips
