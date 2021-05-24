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
    from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.hindi_sanitizer import (
        HindiSanitizer,
    )
    from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.gujarati_sanitizer import (
        GujaratiSanitizer,
    )
    from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.kannada_sanitizer import (
        KannadaSanitizer,
    )
    from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.indian_english_sanitizer import (
        IndianEnglishSanitizer,
    )
    from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.punjabi_sanitizer import (
        PunjabiSanitizer,
    )
    from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.malayalam_sanitizer import (
        MalayalamSanitizer,
    )
    from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.bengali_sanitizer import (
        BengaliSanitizer,
    )
    from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.telugu_sanitizer import (
        TeluguSanitizer,
    )
    from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.assamese_sanitizer import (
        AssameseSanitizer,
    )
    from ekstep_data_pipelines.audio_transcription.transcription_sanitizers.urdu_sanitizer import (
        UrduSanitizer,
    )

    hindi_sanitizer = HindiSanitizer.get_instance(**kwargs)
    gujarati_sanitizer = GujaratiSanitizer.get_instance(**kwargs)
    kannada_sanitizer = KannadaSanitizer.get_instance(**kwargs)
    indian_english_sanitizer = IndianEnglishSanitizer.get_instance(**kwargs)
    punjabi_sanitizer = PunjabiSanitizer.get_instance(**kwargs)
    malayalam_sanitizer = MalayalamSanitizer.get_instance(**kwargs)
    bengali_sanitizer = BengaliSanitizer.get_instance(**kwargs)
    telugu_sanitizer = TeluguSanitizer.get_instance(**kwargs)
    assamese_sanitizer = AssameseSanitizer.get_instance(**kwargs)
    urdu_sanitizer = UrduSanitizer.get_instance(**kwargs)

    return {
        "hindi": hindi_sanitizer,
        "gujarati": gujarati_sanitizer,
        "default": hindi_sanitizer,
        "kannada": kannada_sanitizer,
        "indian_english": indian_english_sanitizer,
        "punjabi": punjabi_sanitizer,
        "malayalam": malayalam_sanitizer,
        "bengali": bengali_sanitizer,
        "telugu": telugu_sanitizer,
        "assamese": assamese_sanitizer,
        "urdu": urdu_sanitizer
    }
