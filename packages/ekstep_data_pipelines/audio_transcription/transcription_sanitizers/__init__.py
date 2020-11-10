from abc import ABCMeta, abstractmethod, abstractclassmethod

class BaseTranscriptionSanitizer(metaclass=ABCMeta):
    """
    Interface for language based Sanitizers
    """

    @abstractmethod
    def sanitize(self, transcription: str):
        pass



def get_transcription_sanitizers(**kwargs):

    # cyclic imports
    from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.hindi_sanitizer import HindiSanitizer
    from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.gujrati_sanitizer import GujratiSanitizer

    hindi_sanitizer = HindiSanitizer.get_instance(**kwargs)
    gujrati_sanitizer = GujratiSanitizer.get_instance(**kwargs)

    return {'hindi': hindi_sanitizer, 'gujrati': gujrati_sanitizer,'default': hindi_sanitizer}
