CREATE TABLE public.source_metadata_downloaded (
    source_id integer NOT NULL,
    source text,
    total_duration double precision,
    num_speaker integer,
    num_of_audio integer,
    staged_for_snr boolean DEFAULT false NOT NULL
);
