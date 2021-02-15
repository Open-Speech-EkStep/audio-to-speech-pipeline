CREATE TABLE public.source_metadata_processed (
    source text NOT NULL,
    num_speaker integer,
    total_duration double precision,
    cleaned_duration double precision,
    num_of_audio integer,
    experiment_use_status boolean DEFAULT false,
    experiment_id integer,
    load_datetime timestamp without time zone DEFAULT CURRENT_TIMESTAMP(2) NOT NULL
);
