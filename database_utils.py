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
        upload_to_db: uploads data to a target database
    '''
    # Takes config file as argument, returns dict (hopefully) of credentials
    def read_db_creds(self, config_file):
        '''
        This function

        Args:

        Returns:
        '''
        with open(config_file, 'r') as creds:
            db_creds = yaml.safe_load(creds)
            return db_creds
        
    # Creates SQLAlchemy engine from credentials, can be activated to
    # perform queries (hopefully)
    def init_db_engine(self, db_creds: dict):
        '''
        This function

        Args:
        
        Returns:
        '''
        engine = create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")  
        return engine
    
    # Connects to the database using the engine and prints a list of
    # the tables within
    def list_db_tables(self, engine: Engine):
        '''
        This function

        Args:
        
        Returns:
        '''
        with engine.connect() as conn:
            inspector = inspect(engine)
            return inspector.get_table_names()
    
    def upload_to_db(self, engine: Engine, dataframe: DataFrame, table_name: str):
        '''
        This function

        Args:
        
        Returns:
        '''
        dataframe.to_sql(table_name, engine, if_exists='replace')