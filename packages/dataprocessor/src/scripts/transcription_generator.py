from .extract_transcription import extract_transcription
from .save_transcription import save_transcription, save_transcriptions
from .transcription_sanitizer import TranscriptionSanitizer

def create_transcriptions(google_speech_client, wav_file_path, output_path, api_response_file):
    response = google_speech_client.call_speech_to_text(wav_file_path, True, output_path, api_response_file)
    transcriptions = extract_transcription(response)
    save_transcriptions(output_path, transcriptions, 'chunk')
    return transcriptions


def create_azure_transcription(azure_client, language, wav_file_path, punctuation=False):
    result = azure_client.speech_to_text(wav_file_path, language)
    transcription_file_path = wav_file_path.replace('.wav', '.txt')
    transcription = TranscriptionSanitizer().sanitize(result.text)
    save_transcription(transcription, transcription_file_path)
    return transcription


def create_google_transcription(google_client, remote_wav_file_path, local_wav_file_path, punctuation=False):
    content = google_client.call_speech_to_text(remote_wav_file_path, False)
    transcription_file_path = local_wav_file_path.replace('.wav', '.txt')
    transcriptions = list(map(lambda c: c.alternatives[0].transcript, content.results))
    transcription = ' '.join(transcriptions)
    transcription = TranscriptionSanitizer().sanitize(transcription)
    save_transcription(transcription, transcription_file_path)
    return transcription