-- depends: 0001.create-public.media_speaker_mapping
CREATE INDEX IF NOT EXISTS idx_media_speaker_mapping_audio_id ON public.media_speaker_mapping USING btree (audio_id);
