import yaml
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from sqlalchemy import inspect
from pandas import DataFrame


class DatabaseConnector:
    '''
    This class provides functionality to interact with databases.

    Functions:
        read_db_creds: reads the credentials of a database from a YAML file.
        init_db_engine: creates an SQLAlchemy engine object for connecting to a database.
        list_db_tables: lists the tables in a database.
        upload_to_db: uploads data to a target database.
    '''

    def read_db_creds(self, credentials_file):
        '''
        This function is used to read database credentials.

        Args:
            credentials_file: YAML file containing the credentials of the database.

        Returns:
            db_creds: dict of the database credentials
        '''
        with open(credentials_file, 'r') as creds:
            db_creds = yaml.safe_load(creds)
            return db_creds
        
    def init_db_engine(self, db_creds: dict):
        '''
        This function creates an SQLAlchemy engine from the database credentials dict.

        Args:
            db_creds: Python dict returned from the read_db_creds function.
        
        Returns:
            engine: SQLAlchemy engine object allowing connection to a database.
        '''
        engine = create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")  
        return engine
    
    def list_db_tables(self, engine: Engine):
        '''
        This function connects to a database and lists the tables it contains.

        Args:
            engine: SQLAlchemy engine object returned from init_db_engine.
        
        Returns:
            inspector.get_table_names(): a list of tables within the database.
        '''
        with engine.connect() as conn:
            inspector = inspect(engine)
            return inspector.get_table_names()
    
    def upload_to_db(self, engine: Engine, dataframe: DataFrame, table_name: str):
        '''
        This function creates a table in the connected database.

        Args:
            engine: SQLAlchemy engine object returned from init_db_engine.
            dataframe: a pandas dataframe of business data.
            table_name: a string representing the name of the table to be created.
        '''
        dataframe.to_sql(table_name, engine, if_exists='replace')