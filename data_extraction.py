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


        # Cleaning card_number column by casting as string, removing non numeric characters
        # Then converting to numeric data and finally dropping null values
        pdf_data['card_number'] = pdf_data['card_number'].astype('string').str.replace(r'[^0-9]+', '', regex=True)
        pdf_data['card_number'] = pd.to_numeric(pdf_data['card_number'])
        null_card_no = pdf_data.loc[pdf_data['card_number'].isnull()]
        pdf_data = pdf_data.drop(null_card_no.index)
        pdf_data['card_number'] = pdf_data['card_number'].astype('int64')


        # Casting exp_date to datetime64
        pdf_data['expiry_date'] = pd.to_datetime(pdf_data['expiry_date'], format='%m/%y', errors='coerce')
        null_exp_date = pdf_data.loc[pdf_data['expiry_date'].isnull()]
        pdf_data = pdf_data.drop(null_exp_date.index)
        pdf_data['expiry_date'] = pdf_data['expiry_date'].dt.date


        # cleaning payment date
        pdf_data['date_payment_confirmed'] = pd.to_datetime(pdf_data['date_payment_confirmed'], format='mixed')
        pdf_data['date_payment_confirmed'] = pdf_data['date_payment_confirmed'].dt.date
        
        # Resets index column on dataframe
        pdf_data.reset_index(drop = True, inplace=True)

        show(pdf)

