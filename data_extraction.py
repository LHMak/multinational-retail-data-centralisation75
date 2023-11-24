# Defining data extractor class
from sqlalchemy import text
from sqlalchemy.engine import Engine
from pandasgui import show
from tqdm import tqdm
import pandas as pd
import tabula
import requests
import boto3
import os



class DataExtractor:
    # Takes engine and table as arguments, returns table contents as a Pandas Dataframe
    # returns this dataframe to main.py
    def read_rds_table(self, engine: Engine, chosen_table):
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {chosen_table}"))
            df_result = pd.DataFrame(result)
            return df_result
        

    def retrieve_pdf_data(self, link):
        # Creates dataframes from a pdf. Returns a dataframe for each page.
        raw_card_data = tabula.read_pdf(link, pages='all')

        # Concats the serparate dataframes together and returns dataframe to main.py
        list_of_dfs = []
        for df in raw_card_data:
            list_of_dfs.append(df)
        raw_card_data = pd.concat(list_of_dfs)
        return raw_card_data
    

    def list_number_of_stores(self,num_stores_endpoint, header_dict):
        # Sends get request to number of stores API endpoint,
        # receives total number of stores and returns this to main.py
        num_stores_response = requests.get(num_stores_endpoint, headers=header_dict)
        num_stores = num_stores_response.json()['number_stores']
        return num_stores
    

    def retrieve_stores_data(self, retrieve_store_endpoint_base, header_dict, num_stores):
        # Uses the number of stores received from list_number_of_stores
        # to send a get requesst to the API for each store.
        # Concats all responses into one dataframe and returns it to main.py 
        store_data_response_list= []
        for store in tqdm(range(num_stores)):
            retrieve_store_endpoint = retrieve_store_endpoint_base + str(store)
            store_data_response = requests.get(retrieve_store_endpoint, headers=header_dict)
            store_data_response_list.append(store_data_response.json())
        store_data = pd.DataFrame.from_records(store_data_response_list, index= 'index') 
        return store_data
    

    def extract_from_s3(self, s3_address):
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

