# Defining database connector class
import yaml

class Database_connector:
    def __init__(self):
        self.db_creds = None
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as creds:
            self.db_creds = yaml.safe_load(creds)
            return self.db_creds
    def init_db_engine(self):
        print(self.db_creds)


Database_connector.read_db_creds('db_creds.yaml')