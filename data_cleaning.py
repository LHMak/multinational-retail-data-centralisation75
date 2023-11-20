import pandas as pd
import numpy as np

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
        print("\n\n--- Casting string columns ---\n\n")
        df_string_cols = list(df[['first_name', 'last_name', 'company', 'email_address',
                             'country', 'country_code', 'user_uuid']])
        df[df_string_cols] = df[df_string_cols].astype('string')
        # replace new line char in address column with ', '
        # down the line, may need to use regex expression instead

        df.info()

        print("\n\n--- Casting date columns ---\n\n")

        # using pd.to_datetime to convert date formats, errors return NaT
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], infer_datetime_format=True, errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], infer_datetime_format=True, errors='coerce')
        # removing rows where date_of_birth + join_date are null
        null_dobs = df['date_of_birth'].isnull()
        null_join_dates = df['join_date'].isnull()
        problem_dates = df[(null_dobs) & (null_join_dates)]
        df = df.drop(problem_dates.index)
        df.info()
        print(df)
        print('cleaning address -------------------')

        print(df['address'].str.split('\n', expand=True))



