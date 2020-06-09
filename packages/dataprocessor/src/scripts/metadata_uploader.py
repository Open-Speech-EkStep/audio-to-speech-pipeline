import os
from os.path import join, dirname
from sqlalchemy import create_engine, select, MetaData, Table, text
from dotenv import load_dotenv

class CloudSQLUploader():

    def __init__(self):
        pass

    def upload_file(self,file_path,db_conn):
        with open(file_path, 'r') as f:
            conn = db_conn.raw_connection()
            cursor = conn.cursor()
            cmd = 'COPY media_metadata_staging(raw_file_name,duration,title,speaker_name,audio_id,cleaned_duration,num_of_speakers,language,has_other_audio_signature,type,source,experiment_use,utterances_files_list,source_url,speaker_gender,source_website,experiment_name,mother_tongue,age_group,recorded_state,recorded_district,recorded_place,recorded_date,purpose) FROM STDIN WITH (FORMAT CSV, HEADER)'
            cursor.copy_expert(cmd, f)
            conn.commit()

    def select_and_print(self,db_conn):
        metadata = MetaData(bind=None)
        table = Table('media_metadata_staging', metadata, autoload=True, autoload_with=db_conn)
        stmt = select([table])
        connection = db_conn.connect()
        results = connection.execute(stmt).fetchall()
        print(results)
