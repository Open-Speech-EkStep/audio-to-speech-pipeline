def upload_file(self, file_path, db_conn):
    print("uploading initiated")
    with open(file_path, 'r') as f:
        conn = db_conn.raw_connection()
        cursor = conn.cursor()
        cmd = 'COPY downloaded_data(raw_file_name,duration,title,speaker_name,audio_id,cleaned_duration,num_of_speakers,language,has_other_audio_signature,type,source,experiment_use,utterances_files_list,source_url,speaker_gender,source_website,experiment_name,mother_tongue,age_group,recorded_state,recorded_district,recorded_place,recorded_date,purpose) FROM STDIN WITH (FORMAT CSV, HEADER)'
        cursor.copy_expert(cmd, f)
        conn.commit()


def upload_file_to_downloaded_source(self, file_path, db_conn):
    print("uploading data to source_metadata")
    with open(file_path, 'r') as f:
        conn = db_conn.raw_connection()
        cursor = conn.cursor()
        cmd = 'COPY source_metadata_downloaded(source,num_speaker,total_duration,num_of_audio) FROM STDIN WITH (FORMAT CSV, HEADER)'
        cursor.copy_expert(cmd, f)
        conn.commit()