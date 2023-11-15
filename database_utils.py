# Defining database connector class
import yaml

class Database_connector:
    def read_db_credentials(file):
        with open('db_creds.yaml', 'r') as creds:
            db_creds = yaml.safe_load(creds)
            print(db_creds)
            #Error when calling method as Python's cwd is c:/Users/liamf

Database_connector.read_db_credentials('db_creds.yaml')