-- depends: 0001.create-public.media_metadata_staging
CREATE INDEX IF NOT EXISTS media_hash_code_index ON public.media_metadata_staging USING btree (media_hash_code);
