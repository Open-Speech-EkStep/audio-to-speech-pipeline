MAX_LOAD_DATE_FOR_MEDIA_QUERY = "SELECT MAX (load_datetime) FROM media;"
INSERT_INTO_MEDIA_TABLE_QUERY = (
    "INSERT INTO media(audio_id,raw_file_name,total_duration,title,"
    "cleaned_duration,num_of_speakers,\
        language,has_other_audio_signature,type,source,source_url,source_website,"
    "utterances_files_list,recorded_state,\
            recorded_district,recorded_place,recorded_date,purpose,load_datetime) SELECT "
    "audio_id,raw_file_name,duration,\
                title,cleaned_duration,num_of_speakers,language,has_other_audio_signature,"
    "type,source,source_url,source_website,\
                utterances_files_list,recorded_state,recorded_district,recorded_place"
    ",recorded_date,purpose,load_datetime \
                FROM media_metadata_staging as o where load_datetime > :max_datetime "
    "AND o.speaker_name is not null\
            AND load_datetime = (select max(load_datetime) from media_metadata_staging s "
    "where s.audio_id = o.audio_id );"
)

INSERT_UNIQUE_SPEAKER_QUERY = (
    "INSERT INTO speaker(speaker_name,source,gender,mother_tongue,"
    "age_group,load_datetime) SELECT t.speaker_name, \
    t.source,min(speaker_gender),min(t.mother_tongue),min(t.age_group),min(t.load_datetime) \
        FROM media_metadata_staging t LEFT JOIN speaker ts ON ts.speaker_name = t.speaker_name"
    " WHERE ts.speaker_name IS NULL and t.speaker_name is not null "
    "group by t.speaker_name, t.source"
)

GET_AUDIO_ID_QUERY = (
    "SELECT media_metadata_staging.audio_id FROM media_metadata_staging"
    " where is_normalized = false and speaker_name is not null"
)

DEFULT_QUERY_FOR_INSERT_INTO_MAPPING_TABLE = (
    "insert into media_speaker_mapping(audio_id, "
    "speaker_id, clipped_utterance_file_name, "
    "clipped_utterance_duration,load_datetime,snr,status"
    ",fail_reason, language_confidence_score) values"
)

GET_SPEAKER_ID_QUERY = (
    "select speaker_id from speaker s JOIN media_metadata_staging b on "
    "s.speaker_name = b.speaker_name \
where b.audio_id = :audio_id;"
)

FETCH_QUERY_WHERE_SPEAKER_IS_NULL = (
    "select audio_id ,utterances_files_list,load_datetime "
    "from media_metadata_staging where is_normalized = false and"
    " speaker_name is null"
)

DEFAULT_INSERT_QUERY = (
    "insert into media_speaker_mapping(clipped_utterance_file_name, clipped_utterance_duration,"
    " audio_id, snr, status,"
    "fail_reason, language_confidence_score, load_datetime) values "
)

DEFAULT_UPDATE_QUERY_FOR_NORMALIZED_FLAG = (
    "update media_metadata_staging set is_normalized = true where audio_id in "
)

GET_LOAD_TIME_FOR_AUDIO_QUERY = (
    "select load_datetime from media where audio_id = :audio_id;"
)

GET_UTTERANCES_LIST_OF_AUDIO_ID = (
    "select utterances_files_list from media where audio_id = :audio_id"
)
