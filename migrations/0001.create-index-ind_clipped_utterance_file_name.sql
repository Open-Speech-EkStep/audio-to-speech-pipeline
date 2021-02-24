-- depends: 0001.create-public.media_speaker_mapping
CREATE INDEX IF NOT EXISTS ind_clipped_utterance_file_name ON public.media_speaker_mapping USING btree (clipped_utterance_file_name);
