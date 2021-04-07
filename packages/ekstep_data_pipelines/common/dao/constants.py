GET_UNIQUE_ID = "SELECT nextval('audio_id_seq');"
IS_EXIST = (
    "select exists(select 1 from media_metadata_staging where raw_file_name= :file_name "
    "or media_hash_code = :hash_code);"
)
COMMAND_WITH_LICENSE = (
    "COPY media_metadata_staging(raw_file_name,duration,title,"
    "speaker_name,audio_id,cleaned_duration,num_of_speakers,language,"
    "has_other_audio_signature,type,source,experiment_use,"
    "utterances_files_list,source_url,speaker_gender,source_website,"
    "experiment_name,mother_tongue,age_group,recorded_state,"
    "recorded_district,recorded_place,recorded_date,purpose,license,"
    "media_hash_code) FROM STDIN WITH (FORMAT CSV, HEADER)"
)
COMMAND_WITHOUT_LICENSE = (
    "COPY media_metadata_staging(raw_file_name,duration,title,"
    "speaker_name,audio_id,cleaned_duration,num_of_speakers,language"
    ",has_other_audio_signature,type,source,experiment_use,"
    "utterances_files_list,source_url,speaker_gender,source_website,"
    "experiment_name,mother_tongue,age_group,recorded_state,"
    "recorded_district,recorded_place,recorded_date,purpose,"
    "media_hash_code) FROM STDIN WITH (FORMAT CSV, HEADER)"
)
LICENSE = "license"
