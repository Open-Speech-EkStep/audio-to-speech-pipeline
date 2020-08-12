CONFIG_NAME = "data_tagger_config"
SPEAKER_CRITERIA = "filter_by_speaker_meta_data"
SOURCE_CRITERIA = "filter_by_source"
FILTER_CRITERIA = "filter_criteria"
NUMBER_OF_SPEAKERS = "number_of_speakers"
DURATION = "number_of_minutes_per_speaker"
SOURCE_NAME = "source_name"
SOURCE = "source"
PROCESS_MODE = "process_mode"
LANDING_PATH = "landing_directory_path"
SOURCE_PATH = "source_directory_path"

# Speaker critieria queries
SELECT_SPEAKER_FOR_DATA_GREATER_THAN_DURATION_QUERY = "select speaker_name from downloaded_data where staged_for_snr = false group by speaker_name having sum(duration) >= :duration  limit :speaker_count"
SELECT_SPEAKER_FOR_DATA_LESS_THAN_DURATION_QUERY = "select speaker_name from downloaded_data where staged_for_snr = false group by speaker_name having sum(duration) <= :duration  limit :speaker_count"

SELECT_SPEAKER_FOR_DATA_GREATER_THAN_DURATION_WITH_SOURCE_QUERY = "select speaker_name from downloaded_data where source = :source_name and staged_for_snr = false group by speaker_name having sum(duration) >= :duration  limit :speaker_count"
SELECT_SPEAKER_FOR_DATA_LESS_THAN_DURATION_WITH_SOURCE_QUERY = "select speaker_name from downloaded_data where source = :source_name and staged_for_snr = false group by speaker_name having sum(duration) <= :duration  limit :speaker_count"

FILE_INFO_UPDATE_QUERY = "update downloaded_data set staged_for_snr = true where raw_file_name in"
FILE_INFO_QUERY = "select source,raw_file_name,duration,speaker_name from downloaded_data where speaker_name in"

# Source critieria query
SOURCE_UPDATE_QUERY = "update source_metadata_downloaded set staged_for_snr = true where source in"
#SOURCE_FILE_PATH = "data/audiotospeech/raw/download/catalogued/hindi/audio"
#DESTINATION_FILE_PATH = "data/audiotospeech/raw/landing/hindi/audio"


CONFIGERATION_DICT = {
    1: {
        'with_source': (SELECT_SPEAKER_FOR_DATA_GREATER_THAN_DURATION_WITH_SOURCE_QUERY, SELECT_SPEAKER_FOR_DATA_LESS_THAN_DURATION_WITH_SOURCE_QUERY),
        'without_source': (SELECT_SPEAKER_FOR_DATA_GREATER_THAN_DURATION_QUERY, SELECT_SPEAKER_FOR_DATA_LESS_THAN_DURATION_QUERY)
    },
    2: {
        'with_source': (SELECT_SPEAKER_FOR_DATA_GREATER_THAN_DURATION_WITH_SOURCE_QUERY, None),
        'without_source': (SELECT_SPEAKER_FOR_DATA_GREATER_THAN_DURATION_QUERY, None)
    },
    3: {
        'with_source': (SELECT_SPEAKER_FOR_DATA_LESS_THAN_DURATION_WITH_SOURCE_QUERY, None),
        'without_source': (SELECT_SPEAKER_FOR_DATA_LESS_THAN_DURATION_QUERY, None)
    }
}