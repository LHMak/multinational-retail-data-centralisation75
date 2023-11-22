# Defining data extractor class
from sqlalchemy import text
from sqlalchemy.engine import Engine
from pandasgui import show
import pandas as pd
import tabula
import requests


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
    
    # Error: <Response [403]>
    def list_number_of_stores(self,num_stores_endpoint, header_dict):
        num_stores_response = requests.get(num_stores_endpoint, header_dict)
        print(f'The request url being made is: {num_stores_response.url}')
        return num_stores_response