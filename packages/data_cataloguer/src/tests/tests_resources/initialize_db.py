def create_db(c,conn):
# Create table experiment -------
    c.execute('''CREATE TABLE experiment
        (experiment_id INTEGER, experiment_name text NOT NULL, experiment_description text)''')


    # Table: media_speaker_mapping
    # Create table media_speaker_mapping -------
    c.execute('''CREATE TABLE media_speaker_mapping
                (audio_id INTEGER, speaker_id INTEGER, clipped_utterance_file_name text,clipped_utterance_duration real,experiment_use_status text,speaker_exp_use_status text,experiment_id INTEGER,load_datetime text)''')

    c.execute("insert into media_speaker_mapping(audio_id, speaker_id, clipped_utterance_file_name, clipped_utterance_duration,load_datetime) values (123,5,'some_name',3,123445),(123,6,'some_name2',3,123)")
    # Table: media
    # Create table media -------
    c.execute('''CREATE TABLE media
                (audio_id INTEGER, raw_file_name text, total_duration real,title text,cleaned_duration real,num_of_speakers real, language text,has_other_audio_signature text, type text,source text,source_url text,source_website text,utterances_files_list text,recorded_state text,recorded_district text,recorded_place text,recorded_date text,purpose text, load_datetime real)''')

    c.execute("insert into media(audio_id,raw_file_name,load_datetime) values (1234 ,'somefile',julianday('now'))")
    # Table: speaker
    # Create table speaker -------
    c.execute('''CREATE TABLE speaker
                (speaker_id INTEGER PRIMARY KEY, speaker_name text,source text,gender text,mother_tongue text,age_group text,voice_signature text,load_datetime text)''')


    # c.execute("insert into speaker(speaker_name, source, gender, load_datetime) values (5,)")

    # Table: media_metadata_staging
    # Create table media_metadata_staging -------
    c.execute('''CREATE TABLE media_metadata_staging
                (raw_file_name text,duration real,title text, speaker_name text,audio_id INTEGER, cleaned_duration real, num_of_speakers real,language text,has_other_audio_signature text, type text,source text,experiment_use text,utterances_files_list text,source_url text,speaker_gender text, source_website text,experiment_name text,mother_tongue text,age_group text,recorded_state text,recorded_district text,recorded_place text,recorded_date text,purpose text, load_datetime real)''')
    # c.commit()
    # c.close()

    c.execute("INSERT INTO media_metadata_staging(raw_file_name ,duration ,title , speaker_name ,audio_id,cleaned_duration,num_of_speakers,language,type,source,experiment_use ,utterances_files_list,load_datetime ) VALUES \
        ('01_Secretes_Of_Happiness_-_Sr._Shreya____13-09-2019.mp3',59.85,'Secretes Of Happiness','sr. shreya1',202006, 38,1, 'hindi' ,'audio','Brahmakumaris','False','[abc,djj,kdks]',julianday('now')),\
            ('01_Secretes_Of_Happiness_-_Sr._Shreya____13-09-2019.mp3',59.85,'Secretes Of Happiness','sr. shreya2',202006, 38,1, 'hindi' ,'audio','Brahmakumaris','False','[abc,djj,kdks]',julianday('now')),\
                ('01_Secretes_Of_Happiness_-_Sr._Shreya____13-09-2019.mp3',59.85,'Secretes Of Happiness','sr. shreya2',202006, 38,1, 'hindi' ,'audio','Brahmakumaris','False','[file1:6,file2:8,file3:9,file4:11]',julianday('now')),\
                    ('01_Secretes_Of_Happiness_-_Sr._Shreya____13-09-2019.mp3',59.85,'Secretes Of Happiness','sr. shreyya3',202006, 38,1, 'hindi' ,'audio','Brahmakumaris','False','[file1:6,file2:8,file3:9,file4:11]',julianday('now'))")
    conn.commit()


def find_count(c,table_name):
    count = c.execute(f'select count(*) from {table_name}').fetchall()
    print(count)
    return count[0][0]

def find_used_speaker_count(c):
    count = c.execute('select count(*) from media_speaker_mapping where speaker_exp_use_status = true').fetchall()
    print(count)
    return count[0][0]