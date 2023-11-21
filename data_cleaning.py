import pandas as pd
import numpy as np
from pandasgui import show
# Defining data cleaner class

class DataCleaning:
    # Create a method called clean_user_data in the DataCleaning
    # class which will perform the cleaning of the user data.
    # You will need clean the user data, look out for NULL
    # values, errors with dates, incorrectly typed values
    # and rows filled with the wrong information.    
    def clean_user_data (self, df):

        # set index to column 1 and sort by ascending
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



