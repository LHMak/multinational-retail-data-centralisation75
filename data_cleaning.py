import pandas as pd
import re
import numpy as np


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

        # Resets index column on dataframe and returns to main.py
        df.reset_index(drop = True, inplace=True)
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


    def convert_product_weights(self, raw_product_data):
        # Defines function to separate quantity and weight from unit then performs
        # calculations to convert the weight to kg, depending on its unit.
        # Two main if/else blocks are used to account for some entries including a
        # quantity value with their weight and unit.
        def convert_to_kg(weight_col):
            # Regex which Looks for a quantity, weight and a unit
            unit_match = re.match(r'([\d.]+)\s*[xX]\s*([\d.]+)\s*([a-zA-Z]+)', weight_col)
            # If 3 groups were matched (quantity, weight, unit), multiply qty by weight
            # then perform unit conversion
            if unit_match:
                quantity, num, unit = unit_match.groups()
                quantity, num = float(quantity), float(num)
            
                total_value = quantity * num
            
                if unit == 'kg':
                    return total_value
                elif unit == 'g' or unit == 'ml':
                    return total_value / 1000
                elif unit == 'oz':
                    return total_value * 0.0283495
                else:
                    return None
            else:
            # If the regex doesn't match 3 groups, match two (weight and unit) then
            # perform unit conversion. If no match at all, return None
                unit_match = re.match(r'([\d.]+)\s*([a-zA-Z]+)', weight_col)
                if unit_match:
                    num, unit = unit_match.groups()
                    num = float(num)

                    if unit == 'kg':
                        return num
                    elif unit == 'g' or unit == 'ml':
                        return num / 1000
                    elif unit == 'oz':
                        return num * 0.0283495
                    else:
                        return None
                else:
                    return None
                
        # Null values have to be converted to string in order for the convert_to_kg
        # function to work. After conversion, these string 'NaNs' are converted
        # back into np.NaN. Afterwards, NaN values are dropped. These rows looked erroneous.
        raw_product_data['weight'] = raw_product_data['weight'].fillna('NaN')
        raw_product_data['weight'] = raw_product_data['weight'].apply(convert_to_kg)
        raw_product_data['weight'] = raw_product_data['weight'].replace('NaN', np.nan)
        raw_product_data['weight'] = pd.to_numeric(raw_product_data['weight'])
        raw_product_data = raw_product_data.dropna(how='any')
        return raw_product_data


    def clean_product_data(self, raw_product_data):
        # Converts product_name, category, EAN, uuid, removed, product_code to string data type
        string_cols = list(raw_product_data[['product_name', 'category', 'EAN', 'uuid', 'removed', 'product_code']])
        raw_product_data[string_cols] = raw_product_data[string_cols].astype('string')

        # Prints unique values in category and removed columns. No erroneous values were returned
        # so these columns don't seem to need cleaning
        print(f'\nAvailable categories are:\n{raw_product_data["category"].unique()}\n\nItem availability statuses are:\n{raw_product_data["removed"].unique()}\n\n')
        
        # Strips product_price column of '£' then converts column into float64. No errors occured,
        # so this column doesn't seem to need cleaning.
        raw_product_data['product_price'] = raw_product_data['product_price'].str.lstrip('£')
        raw_product_data['product_price'] = pd.to_numeric(raw_product_data['product_price'])
        
        # Converts date_added column to datetime64. Earliest and latests dates seem sensible, so it
        # looks like the datet formats were interpreted correctly. No errors or Null values so this
        # column doesn't seem to require cleaning.
        raw_product_data['date_added'] = pd.to_datetime(raw_product_data['date_added'], format='mixed')
        
        # Resets index column on dataframe and returns to main.py
        raw_product_data.reset_index(drop = True, inplace=True)
        return raw_product_data


    def clean_orders_data(self, raw_orders_table):
        # Removes level_0 column which was created during retrieval of the data. Also removes
        # first_name, last_name, 1 columns which were superfluous. Then sets index column to 'index'
        raw_orders_table = raw_orders_table.drop(['level_0', 'first_name', 'last_name', '1'], axis = 1)
        raw_orders_table = raw_orders_table.set_index('index', drop=True)
        
        # Resets index column on dataframe and returns to main.py
        raw_orders_table.reset_index(drop = True, inplace=True)
        return raw_orders_table
    

    def clean_date_events(self, raw_date_events):
        # Converts timestamps column to datetime64, coerces errors to convert them into Null values.
        # Time component is then extracted, otherwise timestamp would include the date of the Unix Epoch. 
        # The rows with Null timestamps were visually checked and confirmed to be erroneous.
        # Function then drops these erroneous rows.
        raw_date_events['timestamp'] = pd.to_datetime(raw_date_events['timestamp'], format='%H:%M:%S', errors='coerce').dt.time
        null_timestamps_mask = raw_date_events['timestamp'].isnull()
        null_timestamps = raw_date_events[null_timestamps_mask]
        raw_date_events = raw_date_events.drop(null_timestamps.index)

        # Converts month column to datetime64, then extracts the month component.
        # Otherwise, Unix epoch year, day and time would be included.
        # No erroneous month values were identified from a visual check.
        raw_date_events['month'] = pd.to_datetime(raw_date_events['month'], format='%m').dt.month
     
        # Converts year column to datetime64, then extracts the year component.
        # Otherwise, Unix epoch month, day and time would be included.
        # No erroneous month values were identified from a visual check.
        raw_date_events['year'] = pd.to_datetime(raw_date_events['year'], format='%Y').dt.year

        # Converts month column to datetime64, then extracts the month component.
        # Otherwise, Unix epoch year, day and time would be included.
        # No erroneous month values were identified from a visual check.
        raw_date_events['day'] = pd.to_datetime(raw_date_events['day'], format='%d').dt.day

        # Returns unique values in time_period column. When printeed, no erroneous
        # values were detected, so no further cleaning required.
        raw_date_events['time_period'].unique()

        # Resets index column on dataframe and returns to main.py        
        raw_date_events.reset_index(drop = True, inplace=True)
        return raw_date_events


        
        