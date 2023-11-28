from sqlalchemy import text
from sqlalchemy.engine import Engine
import pandas as pd
import tabula
import requests
import boto3
import os


class DataExtractor:
    '''
    This class is used to extract data from the various sources.

    Functions:
        read_rds_table: connects to an RDS table and returns all its data as a pandas dataframe.
        retrieve_pdf_data: reads a PDF file and returns a pandas dataframe of its contents.
        list_number_of_stores: connects to an API endpoint, returning the number of stores in the business.
        retrieve_stores_data: connects to an API endpoint, returning date about all stores in the business.
        extract_from_s3: connects to an Amazon S3 bucket and downloads a specified file. Returns
                         a pandas dataframe of the file's content.
    '''
    # Takes engine and table as arguments, returns table contents as a Pandas Dataframe
    # returns this dataframe to main.py
    def read_rds_table(self, engine: Engine, chosen_table):
        '''
        This function retrieves a table and returns it as a pandas dataframe.

        The function connects to an RDS using an SQLAlchemy engine and queries
        the table for all of its contents. This generates a pandas dataframe which
        is returned to main.py.

        Args:
            engine: SQLAlchemy engine returned from the DatabaseConnector.init_db_engine function
            chosen_table: table name index from the list of tables returned from
                          DatabaseConnector.list_db_tables.
       
        Returns:
            table_result: a pandas dataframe of the table's contents.
        '''
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {chosen_table}"))
            table_result = pd.DataFrame(result)
            return table_result
        
    def retrieve_pdf_data(self, link):
        '''
        This function reads a PDF document and returns a pandas dataframe of its contents.

        This function uses Tabula to read a PDF from a link. All pages are returned as a
        separate pandas dataframe. Once all pages have been read, the dataframes are
        concatenated together.

        Args:
            link: a URL link to the PDF document.
        Returns:
            raw_card_data: a pandas dataframe containing all of the data from each
                           page of the document.
        '''
        # Creates dataframes from a pdf. Returns a dataframe for each page.
        raw_card_data = tabula.read_pdf(link, pages='all')
        # Concats the serparate dataframes together and returns dataframe to main.py
        list_of_dfs = []
        for df in raw_card_data:
            list_of_dfs.append(df)
        raw_card_data = pd.concat(list_of_dfs)
        return raw_card_data

    def list_number_of_stores(self,num_stores_endpoint, api_key_header):
        '''
        This function lists the number of stores in the business.

        The function sends a get request endpoint of an API which returns a response
        with the number of stores in the business. This is returned in JSON form.

        Args:
            num_stores_endpoint: a link to the API which returns the number of stores.
            api_key_header: a dictionary containing the API key to authenticate the API
                         transaction.
        Returns:
            num_stores: a JSON response listing the number of stores in the business.
        '''
        # Sends get request to number of stores API endpoint,
        # receives total number of stores and returns this to main.py
        num_stores_response = requests.get(num_stores_endpoint, headers=api_key_header)
        num_stores = num_stores_response.json()['number_stores']
        return num_stores
    
    def retrieve_stores_data(self, retrieve_store_endpoint_base, api_key_header, num_stores):
        '''
        This function retrieves data for each store and returns it as a pandas dataframe.

        The function sends a get request to an API endpoint. A request is made for each store
        in the business, each request returning a dataframe. Once all requests are made, the 
        returned dataframes are concatenated together.

        Due to the number of requests being made, this function can take a few minutes to complete.

        Args:
            retrieve_store_endpoint_base: this is a partial endpoint url. A number representing
                                          a store is added on for each request being made.
        
        Returns:
            store_data: a pandas dataframe containing the data for each store in the business.
        '''
        # Uses the number of stores received from list_number_of_stores
        # to send a get requesst to the API for each store.
        # Concats all responses into one dataframe and returns it to main.py 
        store_data_response_list= []
        for store in range(num_stores):
            retrieve_store_endpoint = retrieve_store_endpoint_base + str(store)
            store_data_response = requests.get(retrieve_store_endpoint, headers=api_key_header)
            store_data_response_list.append(store_data_response.json())
        store_data = pd.DataFrame.from_records(store_data_response_list, index= 'index') 
        return store_data
    
    def extract_from_s3(self, s3_address):
        '''
        This function downloads a file from an S3 bucket and returns it as a pandas dataframe.

        The function takes the URL of the file and uses the string.split() function to
        separate the bucket and file name portions of the URL. These portions are supplied to
        the s3.download_file() function to locate the file to be downloaded.

        The os module is used to access Python's current working directory so that the file save
        path can be supplied to the s3.download_file() function.

        If the file is of CSV or JSON format, it is returned as a pandas dataframe.

        Args:
            s3_address: URL of the file stored in an S3 bucket.
        
        Returns:
            raw_s3_details: a pandas dataframe of the downloaded file.
        '''
        # Downloads s3 file into current working directory (cwd)
        s3_address = s3_address.split('/')
        cwd = os.getcwd()
        save_path = "/".join((cwd, s3_address[-1]))
        file_type = s3_address[-1].split('.')[-1]
        s3 = boto3.client('s3')
        s3.download_file(s3_address[-2], s3_address[-1], save_path)
        # Checks if downloaded file is .csv or .json and returns it as a dataframe.
        # Dataframe is then returned to main.py 
        if file_type == 'csv':
            raw_s3_details = pd.read_csv(s3_address[-1], index_col=0)
        elif file_type == 'json':
            raw_s3_details = pd.read_json(s3_address[-1])
        else:
            raise TypeError(f'Sorry, {file_type} file types are not accepted by this function.\nThis function only works with .csv and .json file types.')
        return raw_s3_details