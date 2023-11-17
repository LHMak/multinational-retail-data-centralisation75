import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
from database_utils import DatabaseConnector

#instantialising DatabaseConnector
connection = DatabaseConnector()

# gather credentials for database from file
creds = connection.read_db_creds('db_creds.yaml')

# create engine from credentials
engine = connection.init_db_engine(creds)

# list table from connected database
connection.list_db_tables(engine)
