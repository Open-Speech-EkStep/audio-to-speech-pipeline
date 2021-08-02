-- depends: 0001.create-public.media_speaker_mapping
ALTER TABLE public.media_speaker_mapping
ADD COLUMN LABELLED_ARTIFACT_NAME VARCHAR


-- depends: 0001.create-public.media_speaker_mapping
ALTER TABLE public.media_speaker_mapping
ADD COLUMN UNLABELLED_ARTIFACT_NAME VARCHAR
