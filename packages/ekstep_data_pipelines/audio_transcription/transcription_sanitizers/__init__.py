from abc import ABCMeta, abstractmethod, abstractclassmethod
from .hindi_sanitizer import HindiSanitizer
from .gujrati_sanitizer import GujratiSanitizer

class BaseTranscriptionSanitizer(metaclass=ABCMeta):
    """
    Interface for language based Sanitizers
    """

    @abstractmethod
    def sanitize(self, transcription: str):
        pass



def get_transcription_sanitizers(**kwargs):
    hindi_sanitizer = HindiSanitizer.get_instance(**kwargs)
    gujrati_sanitizer = GujratiSanitizer.get_instance(**kwargs)

    return {'hindi_sanitizer': hindi_sanitizer, 'gujrati': gujrati_sanitizer,'default': hindi_sanitizer}
