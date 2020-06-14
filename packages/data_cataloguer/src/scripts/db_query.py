GET_EXPERIMENT_ID = "select experiment_id from experiment where experiment_name = :exp_name;"
INSERT_NEW_EXPERIMENT = "insert into experiment(experiment_name,experiment_description) values (:name,:desc) RETURNING experiment_id;"
GET_ALL_SPEAKER_ID_FROM_GIVEN_EXP = "SELECT speaker_id \
        FROM media_speaker_mapping_test_with_yaml where experiment_id = :id group by speaker_id HAVING SUM(clipped_utterance_duration) >= :time limit :require_speaker_from_existing_exp;"
INITIAL_TEXT_OF_INSERT_QUERY = "insert into media_speaker_mapping_test_with_yaml(audio_id, speaker_id, clipped_utterance_file_name, clipped_utterance_duration,\
        load_datetime,experiment_id,experiment_use_status,speaker_exp_use_status) values "
GET_NEW_SPEAKER = "SELECT speaker_id \
        FROM media_speaker_mapping_test_with_yaml where speaker_exp_use_status = false group by speaker_id HAVING SUM(clipped_utterance_duration) >= :time limit :require_speaker;"
GET_UTTERANCES_OF_GIVEN_EXP = "select * from media_speaker_mapping_test_with_yaml where speaker_id = :speaker_id and experiment_use_status = true and experiment_id = :exp_id;"
GET_UTTERANCES_OF_NEW_USER = "select * from media_speaker_mapping_test_with_yaml where speaker_id = :speaker_id and experiment_use_status = false;"
INITIAL_TEXT_OF_UPDATE_QUERY = "update media_speaker_mapping_test_with_yaml set speaker_exp_use_status = true WHERE speaker_id IN ( "

GET_ALL_DATA_OF_CURRENT_EXP = "SELECT msm.audio_id, msm.clipped_utterance_file_name, media.source,experiment.experiment_name \
        FROM media_speaker_mapping_test_with_yaml msm \
        LEFT JOIN media ON media.audio_id = msm.audio_id \
        LEFT JOIN experiment ON msm.experiment_id = experiment.experiment_id \
        WHERE msm.experiment_use_status = true and msm.experiment_id = :exp_id;"
