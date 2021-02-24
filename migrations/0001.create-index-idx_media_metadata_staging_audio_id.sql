-- depends: 0001.create-public.media_metadata_staging
CREATE INDEX IF NOT EXISTS idx_media_metadata_staging_audio_id ON public.media_metadata_staging USING btree (audio_id);
