GET_UNIQUE_ID = "SELECT nextval('audio_id_seq');"
IS_EXIST = "select exists(select 1 from media_metadata_staging where raw_file_name= :file_name or media_hash_code = :hash_code);"