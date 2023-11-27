from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


# instantialising DatabaseConnector
connection = DatabaseConnector()
# instantialising DataExtractor
extractor = DataExtractor()
# instantialising DataCleaning
cleaner = DataCleaning()
# Assigning database credentials to variable
rds_db_creds = 'db_creds.yaml'
sales_data_creds = 'sales_data_creds.yaml'
# Assigning endpoints and header dictionary to connect to store data API
num_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

# Connects to Amazon RDS and retrieves raw user_data,
# converts it to a dataframe, then returns cleaned user_data
def upload_user_data():
    '''
    This function

    Args:
        
    Returns:
    '''
    # Retrieve and clean user_data from RDS
    rds_creds = connection.read_db_creds(rds_db_creds) # gather database credentials from file
    rds_engine = connection.init_db_engine(rds_creds) # create engine from credentials
    table_list = connection.list_db_tables(rds_engine)# retrieve list of tables in database
    user_data = table_list[1] # index list of tables to select user data
    raw_user_data_df = extractor.read_rds_table(rds_engine, user_data) # return dataframe of user data
    clean_user_data_df = cleaner.clean_user_data(raw_user_data_df) # clean user data

    # Upload cleaned user data to sales_data database
    sales_db_creds = connection.read_db_creds(sales_data_creds) # gather database credentials from file
    sales_db_engine = connection.init_db_engine(sales_db_creds) # create engine from credentials
    connection.upload_to_db(sales_db_engine, clean_user_data_df, 'dim_users') # upload clean data to sales_data database
    
def upload_card_data():
    '''
    This function

    Args:
        
    Returns:
    '''
    # Retrieve and clean card payment data from pdf in link
    link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    raw_card_data = extractor.retrieve_pdf_data(link)
    clean_card_data = cleaner.clean_card_data(raw_card_data)

    # Upload cleaned user data to sales_data database
    sales_db_creds = connection.read_db_creds(sales_data_creds) # gather database credentials from file
    sales_db_engine = connection.init_db_engine(sales_db_creds) # create engine from credentials
    connection.upload_to_db(sales_db_engine, clean_card_data, 'dim_card_details') # upload clean data to sales_data database

def list_num_stores():
    '''
    This function

    Args:
        
    Returns:
    '''
    num_stores = extractor.list_number_of_stores(num_stores_endpoint, header_dict)
    return num_stores

def upload_store_data():
    '''
    This function

    Args:
        
    Returns:
    '''
    # Retrieves number of stores from num_stores_endpoint and extracts store data for each store
    retrieve_store_endpoint_base = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
    num_stores = list_num_stores()
    store_data = extractor.retrieve_stores_data(retrieve_store_endpoint_base, header_dict, num_stores)
    clean_store_data = cleaner.clean_store_data(store_data)

    # Uploads cleaned store data to sales database
    sales_db_creds = connection.read_db_creds(sales_data_creds) # gather database credentials from file
    sales_db_engine = connection.init_db_engine(sales_db_creds) # create engine from credentials
    connection.upload_to_db(sales_db_engine, clean_store_data, 'dim_store_details') # upload clean data to sales_data database

def upload_product_details():
    '''
    This function

    Args:
        
    Returns:
    '''
    # Retrieves raw_product_details from AWS s3 bucket
    product_address = 's3://data-handling-public/products.csv'
    raw_product_details = extractor.extract_from_s3(product_address)

    # Converts product weights into kg then cleans data
    clean_weight_product_details = cleaner.convert_product_weights(raw_product_details)
    clean_product_details = cleaner.clean_product_data(clean_weight_product_details)

    # Uploads cleaned product data to sales database
    sales_db_creds = connection.read_db_creds(sales_data_creds) # gather database credentials from file
    sales_db_engine = connection.init_db_engine(sales_db_creds) # create engine from credentials
    connection.upload_to_db(sales_db_engine, clean_product_details, 'dim_products') # upload clean data to sales_data database

def upload_orders_table():
    '''
    This function

    Args:
        
    Returns:
    '''
    # Retrieves list of tables stores in AWS RDS, then selects and returns the orders_table.
    # Raw orders_table is then cleaned and uploaded to sales_data database
    rds_creds = connection.read_db_creds(rds_db_creds) # gather database credentials from file
    rds_engine = connection.init_db_engine(rds_creds) # create engine from credentials
    table_list = connection.list_db_tables(rds_engine)# retrieve list of tables in database
    order_table = table_list[2] # index list of tables to select orders_table
    raw_orders_table = extractor.read_rds_table(rds_engine, order_table) # return dataframe of orders_table
    clean_orders_table = cleaner.clean_orders_data(raw_orders_table) # clean orders_data

    # Uploads cleaned product data to sales database
    sales_db_creds = connection.read_db_creds(sales_data_creds) # gather database credentials from file
    sales_db_engine = connection.init_db_engine(sales_db_creds) # create engine from credentials
    connection.upload_to_db(sales_db_engine, clean_orders_table, 'orders_table') # upload clean order_table to sales_data database

def upload_date_events():
    '''
    This function

    Args:
        
    Returns:
    '''
    # Retrieves date events json file from AWS s3 bucket
    # Raw date events file is then cleaned and uploaded to sales_data database
    date_events_address = 's3://data-handling-public/date_details.json'
    raw_date_events = extractor.extract_from_s3(date_events_address)
    clean_date_events = cleaner.clean_date_events(raw_date_events)

    # Uploads cleaned product data to sales database
    sales_db_creds = connection.read_db_creds(sales_data_creds) # gather database credentials from file
    sales_db_engine = connection.init_db_engine(sales_db_creds) # create engine from credentials
    connection.upload_to_db(sales_db_engine, clean_date_events, 'dim_date_times') # upload clean data to sales_data database

upload_user_data()
print("User data has now been cleaned and uploaded to the PostgreSQL database.")

upload_card_data()
print("Card data has now been cleaned and uploaded to the PostgreSQL database.")

upload_store_data()
print("Store data has now been cleaned and uploaded to the PostgreSQL database.")

upload_product_details()
print("Product details have now been cleaned and uploaded to the PostgreSQL database.")  

upload_orders_table()
print("Order data has now been cleaned and uploaded to the PostgreSQL database.")

upload_date_events()
print("Date event date has now been cleaned and uploaded to the PostgreSQL database.")
print("All data has now been cleaned and uploaded to the PostgreSQL database!")