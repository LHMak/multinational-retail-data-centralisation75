# Defining data extractor class
from sqlalchemy import text
from sqlalchemy.engine import Engine
import pandas as pd
import tabula
import numpy as np
from pandasgui import show

class DataExtractor:
    # Takes engine and table as arguments, returns table contents as a Pandas Dataframe
    def read_rds_table(self, engine: Engine, chosen_table):
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {chosen_table}"))
            df_result = pd.DataFrame(result)
            # print(df_result.head(10))
            return df_result
    def retrieve_pdf_data(self, link):
        # Reads pdf a dataframe. Each page will be a separate dataframe.
        pdf_data = tabula.read_pdf(link, pages='all')
        # Will have to concat the serparate dataframes
        list_of_dfs = []
        for df in pdf_data:
            list_of_dfs.append(df)
        pdf_data = pd.concat(list_of_dfs)
        # Resets index column on dataframe
        pdf_data.reset_index(drop = True, inplace=True)

        # Casting columns as relevant data types
        stripped_card_no = pdf_data['card_number'].astype('string').str.replace('[^0-9]', '', regex=True)
        #stripped_card_no = stripped_card_no.str.strip('?NULL')
        stripped_card_no= stripped_card_no.fillna(value=np.nan)
        stripped_card_no = stripped_card_no.astype('float64')
        # ValueError: could not convert string to float: ''
        stripped_card_no.info()
