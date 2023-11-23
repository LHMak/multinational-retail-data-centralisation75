import pandas as pd
import numpy as np
from pandasgui import show

class DataCleaning:

    def clean_user_data (self, df):

        # Sets index to column 1 and sort by ascending
        df.set_index(df.columns[0], inplace = True)
        df.sort_values(by=['index'], inplace = True)

        # Casting string columns
        df['address'] = df['address'].str.replace('\n', ', ')
        df_string_cols = list(df[['first_name', 'last_name', 'company', 'email_address',
                             'country', 'country_code', 'user_uuid', 'address', 'phone_number']])
        df[df_string_cols] = df[df_string_cols].astype('string')

        # Casting date columns
        # using pd.to_datetime to convert date formats, errors return NaT
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], infer_datetime_format=True, errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], infer_datetime_format=True, errors='coerce')
        
        # removing rows where date_of_birth or join_date are null
        null_dobs = df['date_of_birth'].isnull()
        null_join_dates = df['join_date'].isnull()
        problem_dates = df[(null_dobs) | (null_join_dates)]
        df = df.drop(problem_dates.index)
        return df
    

    def clean_card_data(self, raw_card_data):
        # Cleans card_number column by casting as string and removing non numeric characters,
        # then converts to numeric data and finally drops null values
        raw_card_data['card_number'] = raw_card_data['card_number'].astype('string').str.replace(r'[^0-9]+', '', regex=True)
        raw_card_data['card_number'] = pd.to_numeric(raw_card_data['card_number'])
        null_card_no = raw_card_data.loc[raw_card_data['card_number'].isnull()]
        raw_card_data = raw_card_data.drop(null_card_no.index)
        raw_card_data['card_number'] = raw_card_data['card_number'].astype('int64')

        # Casts exp_date as datetime64, drops null values then formats as mm/yy
        raw_card_data['expiry_date'] = pd.to_datetime(raw_card_data['expiry_date'], format='%m/%y', errors='coerce')
        null_exp_date = raw_card_data.loc[raw_card_data['expiry_date'].isnull()]
        raw_card_data = raw_card_data.drop(null_exp_date.index)
        raw_card_data['expiry_date'] = raw_card_data['expiry_date'].dt.strftime('%m/%y')

        # Cassts date_payment_confirmed as datetime64, then removing time component
        raw_card_data['date_payment_confirmed'] = pd.to_datetime(raw_card_data['date_payment_confirmed'], format='mixed')
        raw_card_data['date_payment_confirmed'] = raw_card_data['date_payment_confirmed'].dt.date
        
        # Resets index column on dataframe and returns to main.py
        raw_card_data.reset_index(drop = True, inplace=True)
        return raw_card_data
    

    def clean_store_data(self, raw_store_data):

        # Removes 'lat' column as it looks erroneous
        raw_store_data = raw_store_data.drop(['lat'], axis=1)

        # Strips leading 'ee' string on continent column, then drops rows without valid continents
        raw_store_data['continent'] = raw_store_data['continent'].str.lstrip('ee')     
        raw_store_data = raw_store_data.drop(raw_store_data[~raw_store_data['continent'].isin(['Europe','America','nan'])].index)

        # Converts numeric cols to numeric datatype
        numeric_cols = ['longitude', 'latitude', 'staff_numbers']
        raw_store_data[numeric_cols] = raw_store_data[numeric_cols].astype('string')
        raw_store_data[numeric_cols] = raw_store_data[numeric_cols].replace(r'[^0-9]+', '', regex=True)
        raw_store_data[numeric_cols] = raw_store_data[numeric_cols].apply(pd.to_numeric, errors='coerce')
        
        # Converts open_date to datetime64 datatype
        raw_store_data['opening_date'] = pd.to_datetime(raw_store_data['opening_date'], format='mixed', errors='coerce')
        
        # Resets index column on dataframe and returns to main.py
        raw_store_data.reset_index(drop = True, inplace=True)
        return raw_store_data



