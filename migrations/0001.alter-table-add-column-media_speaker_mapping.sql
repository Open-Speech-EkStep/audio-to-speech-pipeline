-- depends: 0001.create-public.media_speaker_mapping
ALTER TABLE media_speaker_mapping
ADD COLUMN data_type VARCHAR(10) DEFAULT null;
