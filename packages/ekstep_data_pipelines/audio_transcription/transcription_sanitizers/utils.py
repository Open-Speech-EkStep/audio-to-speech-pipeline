def get_transcription_sanitizers(**kwargs):
    hindi_sanitizer = HindiSanitizer.get_instance(**kwargs)
    gujrati_sanitizer = GujratiSanitizer.get_instance(**kwargs)
    kannada_sanitizer = KannadaSanitizer.get_instance(**kwargs)
    indian_english_sanitizer = IndianEnglishSanitizer.get_instance(**kwargs)

    return {
        "hindi_sanitizer": hindi_sanitizer,
        "gujrati": gujrati_sanitizer,
        "default": hindi_sanitizer,
        "kannada": kannada_sanitizer,
        "indian_english": indian_english_sanitizer,
    }
