-- depends: 0001.create-public.media_metadata_staging
ALTER TABLE media_metadata_staging
ADD COLUMN data_set_used_for VARCHAR(10) DEFAULT null;
