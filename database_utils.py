# Defining database connector class
import yaml
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from sqlalchemy import inspect

class DatabaseConnector:
    # Takes config file as argument, returns dict (hopefully) of credentials
    def read_db_creds(self, config_file):
        with open(config_file, 'r') as creds:
            db_creds = yaml.safe_load(creds)
            return db_creds
    # Creates SQLAlchemy engine from credentials, can be activated to
    # perform queries (hopefully)
    def init_db_engine(self, db_creds: dict):
        engine = create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")  
        return engine
    # Connects to the database using the engine and prints a list of
    # the tables within
    def list_db_tables(self, engine: Engine):
        with engine.connect() as conn:
            inspector = inspect(engine)
            return inspector.get_table_names()