-- depends: 0001.create-public.media_speaker_mapping
ALTER TABLE media_speaker_mapping
ADD COLUMN is_transcribed boolean,
ADD COLUMN stt_api_used text[] default '{google}';
