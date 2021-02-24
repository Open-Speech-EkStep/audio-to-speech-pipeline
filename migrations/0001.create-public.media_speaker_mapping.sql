CREATE TABLE public.media_speaker_mapping (
    speaker_id integer,
    clipped_utterance_file_name text,
    clipped_utterance_duration double precision,
    experiment_use_status boolean,
    load_datetime timestamp without time zone,
    experiment_id integer,
    audio_id bigint,
    speaker_exp_use_status boolean,
    snr double precision,
    status text,
    utterance_file_path text,
    fail_reason text,
    staged_for_transcription boolean DEFAULT false,
    language_confidence_score json,
    was_noise boolean,
    speaker_gender "char"
);
