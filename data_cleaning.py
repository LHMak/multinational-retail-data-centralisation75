import pandas as pd
import numpy as np
from dateutil.parser import parse

# Defining data cleaner class

class DataCleaning:
    # Create a method called clean_user_data in the DataCleaning
    # class which will perform the cleaning of the user data.
    # You will need clean the user data, look out for NULL
    # values, errors with dates, incorrectly typed values
    # and rows filled with the wrong information.    
    def clean_user_data (self, df):
        print("--- raw df info ---")
        df.info()
        print("\n--- Casting string columns ---\n")
        df_string_cols = list(df[['first_name', 'last_name', 'company', 'email_address',
                             'country', 'country_code', 'user_uuid']])
        df[df_string_cols] = df[df_string_cols].astype('string')
        # replace new line char in address column with ', '
        # down the line, may need to use regex expression instead
        df['address'] = df['address'].str.replace('\n',', ')
        df.info()
        print(df.head(10))

        print("\n--- Casting date columns ---\n")
        # using parse to catch date formats that pd.to_datetime would miss
        df['date_of_birth'] = df['date_of_birth'].apply(parse)
        # parse raises following error:
        # dateutil.parser._parser.ParserError: Unknown string format: KBTI7FI7Y3

        # using pd.to_datetime to convert date formats, errors return NaT
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], infer_datetime_format=True, errors='coerce')

        df.info()
