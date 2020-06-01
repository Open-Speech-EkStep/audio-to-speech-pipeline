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
            cmd = 'COPY media_metadata_staging FROM STDIN WITH (FORMAT CSV, HEADER)'
            cursor.copy_expert(cmd, f)
            conn.commit()

    def select_and_print(self,db_conn):
        metadata = MetaData(bind=None)
        table = Table('media_metadata_staging', metadata, autoload=True, autoload_with=db_conn)
        stmt = select([table])
        connection = db_conn.connect()
        results = connection.execute(stmt).fetchall()
        print(results)
