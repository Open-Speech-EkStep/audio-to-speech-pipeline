import yaml
from sqlalchemy import create_engine, select, MetaData, Table, text


class PostgresClient:
    """
    PostgresClient for DB related operations
    1. Load Configeration
    2. execute select
    3. execute update
    4. execute batch updates
    """

    @staticmethod
    def get_instance(intialization_dict):
        data_processor = PostgresClient(**intialization_dict)
        data_processor.setup_peripherals()
        return data_processor

    def __init__(self, **kwargs):
        self.config_file_path = kwargs.get('config_file_path')
        self.config_dict = None
        self.db = None
        self._connection = None

    def setup_peripherals(self):

        # get yaml config
        self.load_configeration()
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
        db_configuration = self.config_dict.get('common', {}).get('db_configuration', {})
        db_name = db_configuration.get('db_name')
        db_user = db_configuration.get('db_user')
        db_pass = db_configuration.get('db_pass')
        cloud_sql_connection_name = db_configuration.get('cloud_sql_connection_name')

        valid_config = all([db_name, db_user, db_pass, cloud_sql_connection_name])

        if not valid_config:
            # TODO: Raise DB config missing exception
            pass

        self.db = create_engine(
            f'postgresql://{db_user}:{db_pass}@{cloud_sql_connection_name}/{db_name}')

    def load_configeration(self):
        """
        Load up configeration
        """

        if not self.config_file_path:
            # TODO: ideally raise exception here
            pass

        with open(self.config_file_path, 'r') as file:
            parent_config_dict = yaml.load(file)
            self.config_dict = parent_config_dict.get('config')

    def execute_query(self, query, **parm_dict):
        return self.connection.execute(query, **parm_dict).fetchall()

    def execute_update(self, query, **parm_dict):
        return self.connection.execute(query, **parm_dict)