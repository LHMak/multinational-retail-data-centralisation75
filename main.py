import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

#instantialising DatabaseConnector
connection = DatabaseConnector()
#instantialising DataExtractor
extractor = DataExtractor()

# gather credentials for database from file
creds = connection.read_db_creds('db_creds.yaml')

# create engine from credentials
engine = connection.init_db_engine(creds)

# list table from connected database
table_list = connection.list_db_tables(engine)

table_read = extractor.read_rds_table(table_list, engine)
