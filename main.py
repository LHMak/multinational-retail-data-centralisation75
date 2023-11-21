from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd

# instantialising DatabaseConnector
connection = DatabaseConnector()
# instantialising DataExtractor
extractor = DataExtractor()
# instantialising DataCleaning
cleaner = DataCleaning()
# Assigning database credentials to variable
rds_db_creds = 'db_creds.yaml'
sales_data_creds = 'sales_data_creds.yaml'


# This method displays a list of all tables in Amazon RDS
# and prompts the user to choose one, returning
# a Pandas dataframe of the db
def choose_table():
    # gather database credentials from file
    rds_creds = connection.read_db_creds(rds_db_creds)
    # create engine from credentials
    engine = connection.init_db_engine(rds_creds)
    # list tables from connected database
    table_list = connection.list_db_tables(engine)
    chosen_table = input(f"Select a table to view from:\n{table_list}: ")
    # return dataframe of chosen data contents
    table_read = extractor.read_rds_table(engine, chosen_table)
#choose_table()

# Connects to Amazon RDS and retrieves raw user_data,
# converts it to a dataframe, then returns cleaned user_data
def user_data_cleaner():
    # Retrieve and clean user_data from RDS
    rds_creds = connection.read_db_creds(rds_db_creds) # gather database credentials from file
    rds_engine = connection.init_db_engine(rds_creds) # create engine from credentials
    table_list = connection.list_db_tables(rds_engine)# retrieve list of tables in database
    user_data = table_list[1] # index list of tables to select user data
    raw_user_data_df = extractor.read_rds_table(rds_engine, user_data) # return dataframe of user data
    clean_user_data_df = cleaner.clean_user_data(raw_user_data_df) # clean user data

    # Upload cleaned user data to local database
    sales_db_creds = connection.read_db_creds(sales_data_creds) # gather database credentials from file
    local_engine = connection.init_db_engine(sales_db_creds) # create engine from credentials
    connection.upload_to_db(local_engine, clean_user_data_df, 'dim_users') # upload clean data to local database
    
user_data_cleaner()




