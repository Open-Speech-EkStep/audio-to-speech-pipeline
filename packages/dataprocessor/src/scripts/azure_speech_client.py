from azure.cognitiveservices import speech

class AzureSpeechClient(object):

    def __init__(self, speech_key, service_region):
        self.speech_key = speech_key
        self.service_region = service_region

    def speech_to_text(self, audio_file_path, language):
        speech_config = speech.SpeechConfig(subscription=self.speech_key, region=self.service_region)
        audio_input = speech.audio.AudioConfig(filename=audio_file_path)

        speech_recognizer = speech.SpeechRecognizer(speech_config=speech_config, language=language,
                                                       audio_config=audio_input)
        print("Recognizing first result...")

        result = speech_recognizer.recognize_once()

        if result.reason == speech.ResultReason.RecognizedSpeech:
            print("Recognized: {}".format(result.text))
            return result
        elif result.reason == speech.ResultReason.NoMatch:
            print("No speech could be recognized: {}".format(result.no_match_details))
        elif result.reason == speech.ResultReason.Canceled:
            cancellation_details = result.cancellation_details
            print("Speech Recognition canceled: {}".format(cancellation_details.reason))
        print('done..')
