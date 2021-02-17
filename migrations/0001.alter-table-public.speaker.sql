-- depends: 0001.create-public.speaker.sql

ALTER TABLE public.speaker ALTER COLUMN speaker_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.speaker_speaker_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);
