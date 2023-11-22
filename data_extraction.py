# Defining data extractor class
from sqlalchemy import text
from sqlalchemy.engine import Engine
from pandasgui import show
import pandas as pd
import tabula
import requests
from tqdm import tqdm


class DataExtractor:
    # Takes engine and table as arguments, returns table contents as a Pandas Dataframe
    def read_rds_table(self, engine: Engine, chosen_table):
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {chosen_table}"))
            df_result = pd.DataFrame(result)
            return df_result
        

    def retrieve_pdf_data(self, link):
        # Reads pdf a dataframe. Returns a dataframe for each page.
        raw_card_data = tabula.read_pdf(link, pages='all')

        # Concats the serparate dataframes together
        list_of_dfs = []
        for df in raw_card_data:
            list_of_dfs.append(df)
        raw_card_data = pd.concat(list_of_dfs)
        return raw_card_data
    

    def list_number_of_stores(self,num_stores_endpoint, header_dict):
        num_stores_response = requests.get(num_stores_endpoint, headers=header_dict)
        num_stores = num_stores_response.json()['number_stores']
        return num_stores
    

    def retrieve_stores_data(self, retrieve_store_endpoint_base, header_dict, num_stores):
        store_data_response_list= []
        for store in tqdm(range(num_stores)):
            retrieve_store_endpoint = retrieve_store_endpoint_base + str(store)
            store_data_response = requests.get(retrieve_store_endpoint, headers=header_dict)
            store_data_response_list.append(store_data_response.json())
        store_data = pd.DataFrame.from_records(store_data_response_list, index= 'index') 
        return store_data

