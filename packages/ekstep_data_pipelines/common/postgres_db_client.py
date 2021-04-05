import numpy as np
from psycopg2._json import Json
from psycopg2.extensions import register_adapter, AsIs
from sqlalchemy import create_engine, text


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


def addapt_numpy_float32(numpy_float32):
    return AsIs(numpy_float32)


def addapt_numpy_int32(numpy_int32):
    return AsIs(numpy_int32)


def addapt_numpy_array(numpy_array):
    return AsIs(tuple(numpy_array))


register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)
register_adapter(np.float32, addapt_numpy_float32)
register_adapter(np.int32, addapt_numpy_int32)
register_adapter(np.ndarray, addapt_numpy_array)
register_adapter(dict, Json)


class PostgresClient:
    """
    PostgresClient for DB related operations
    1. Load Configeration
    2. execute select
    3. execute update
    4. execute batch updates
    """

    GET_UNIQUE_ID = "SELECT nextval('audio_id_seq');"
    IS_EXIST = "select exists(select 1 from media_metadata_staging where raw_file_name= " \
               ":file_name or media_hash_code = :hash_code);"

    @staticmethod
    def get_instance(config_dict, **kwargs):
        data_processor = PostgresClient(config_dict, **kwargs)
        data_processor.setup_peripherals()
        return data_processor

    def __init__(self, config_dict, **kwargs):
        self.config_dict = config_dict
        self.db = None
        self._connection = None

    def setup_peripherals(self):
        self.setup_db_access()

    @property
    def connection(self):
        if self._connection:
            return self._connection

        if not self.db:
            self.setup_db_access()

        self._connection = self.db.connect()
        return self._connection

    def setup_db_access(self):
        """
        Function for setting up the database access
        """
        db_configuration = self.config_dict.get("common", {}).get(
            "db_configuration", {}
        )
        db_name = db_configuration.get("db_name")
        db_user = db_configuration.get("db_user")
        db_pass = db_configuration.get("db_pass")
        cloud_sql_connection_name = db_configuration.get(
            "cloud_sql_connection_name")

        valid_config = all(
            [db_name, db_user, db_pass, cloud_sql_connection_name])

        if not valid_config:
            # TODO: Raise DB config missing exception
            pass

        self.db = create_engine(
            f"postgresql://{db_user}:{db_pass}@{cloud_sql_connection_name}/{db_name}"
        )

    def execute_query(self, query, **parm_dict):
        return self.connection.execute(text(query), **parm_dict).fetchall()

    def execute_update(self, query, **parm_dict):
        return self.connection.execute(text(query), **parm_dict)

    def execute_batch(self, query, data_list):
        conn = self.db.raw_connection()
        cur = conn.cursor()
        cur.executemany(query, data_list)
        updated_rows = cur.rowcount
        conn.commit()
        cur.close()
        return updated_rows

    def get_unique_id(self):
        return self.connection.execute(self.GET_UNIQUE_ID).fetchall()[0][0]

    def check_file_exist_in_db(self, file_name, hash_code):
        return self.connection.execute(
            text(self.IS_EXIST), file_name=file_name, hash_code=hash_code
        ).fetchall()[0][0]
