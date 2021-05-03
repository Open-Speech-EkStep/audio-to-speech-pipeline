def get_transcription_sanitizers(**kwargs):
    hindi_sanitizer = HindiSanitizer.get_instance(**kwargs)
    gujrati_sanitizer = GujratiSanitizer.get_instance(**kwargs)
    kannada_sanitizer = KannadaSanitizer.get_instance(**kwargs)
    indian_english_sanitizer = IndianEnglishSanitizer.get_instance(**kwargs)
    punjabi_sanitizer = PunjabiSanitizer.get_instance(**kwargs)
    malayalam_sanitizer = MalayalamSanitizer.get_instance(**kwargs)
    bengali_sanitizer = BengaliSanitizer.get_instance(**kwargs)
    telugu_sanitizer = TeluguSanitizer.get_instance(**kwargs)
    assamese_sanitizer = AssameseSanitizer.get_instance(**kwargs)

    return {
        "hindi_sanitizer": hindi_sanitizer,
        "gujrati": gujrati_sanitizer,
        "default": hindi_sanitizer,
        "kannada": kannada_sanitizer,
        "indian_english": indian_english_sanitizer,
        "punjabi": punjabi_sanitizer,
        "malayalam": malayalam_sanitizer,
        "bengali": bengali_sanitizer,
        "telugu": telugu_sanitizer,
        "assamese": assamese_sanitizer
    }
