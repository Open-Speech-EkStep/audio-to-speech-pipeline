CREATE TABLE IF NOT EXISTS public.speaker (
    source text,
    gender text,
    mother_tongue text,
    age_group text,
    voice_signature text,
    speaker_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
    ),
    speaker_name text,
    load_datetime timestamp without time zone DEFAULT CURRENT_TIMESTAMP(2)
);
