# Defining database connector class
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect

class Database_connector:
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as creds:
            db_creds = yaml.safe_load(creds)
            return db_creds
    def init_db_engine(self):
        db_creds = self.read_db_creds()
        engine = create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")  
        return engine
    def list_db_tables(self):
        engine = self.init_db_engine()
        with engine.connect() as connection:
            inspector = inspect(engine)
            print(inspector.get_table_names())


# --- testing functions below to see if they work ---

connect_to_db = Database_connector()
connect_to_db.list_db_tables()