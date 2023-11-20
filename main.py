from database_utils import DatabaseConnector
from data_extraction import DataExtractor

#instantialising DatabaseConnector
connection = DatabaseConnector()
#instantialising DataExtractor
extractor = DataExtractor()
# Assigning database credentials to variable
legacy_db_creds = 'db_creds.yaml'


# This method displays a list of all tables in db
# and prompts the user to choose one, returning
# a Pandas dataframe of the db
def choose_table():
    # gather database credentials from file
    creds = connection.read_db_creds(legacy_db_creds)
    # create engine from credentials
    engine = connection.init_db_engine(creds)
    # list tables from connected database
    table_list = connection.list_db_tables(engine)
    chosen_table = input(f"Select a table to view from:\n{table_list}: ")
    # return dataframe of chosen data contents
    table_read = extractor.read_rds_table(engine, chosen_table)
#choose_table()


# Once complete, this function will create a dataframe
# of the userdata and perform data cleaning on it.
# Currently, this function creates the dataframe
# TODO: add data cleaning functionality
def clean_user_data():
    # gather database credentials from file
    creds = connection.read_db_creds(legacy_db_creds)
    # create engine from credentials
    engine = connection.init_db_engine(creds)
    # gather a list of tables in db
    table_list = connection.list_db_tables(engine)
    # index list of tables to select user data
    user_data = table_list[1]
    # return dataframe of chosen data contents
    table_read = extractor.read_rds_table(engine, user_data)

clean_user_data()



