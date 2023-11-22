# Defining data extractor class
from sqlalchemy import text
from sqlalchemy.engine import Engine
from pandasgui import show
#from datetime import date as dt
import pandas as pd
import tabula
import numpy as np


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
