import pandas as pd
import re
import numpy as np


class DataCleaning:
    '''
    This class is used to clean raw data before it is uploaded to a database.

    Functions:
        clean_user_data: cleans raw user data.
        clean_card_data: cleans raw card data.
        clean_store_data: cleans raw store data.
        convert_product_weights: cleans product weight data.
            convert_to_kg: nested method to convert product weights to kg.
        clean_product_data: cleans raw product data.
        clean_orders_data: cleans raw orders data.
        clean_date_events: cleans raw date events data.
    '''
    def clean_user_data (self, raw_user_data):
        '''
        This function takes in raw user data and cleans it.

        Columns are converted to their intended data types and null values are identified and removed.

        Args:
            raw_user_data: a pandas dataframe containing the raw user data to be cleaned.
        
        Returns:
            raw_user_data: a pandas dataframe which as now been cleaned. Will be reassigned as clean_user_data in main.py
        '''
        # Sets index to column 1 and sort by ascending
        raw_user_data.set_index(raw_user_data.columns[0], inplace = True)
        raw_user_data.sort_values(by=['index'], inplace = True)
        # Casting string columns
        raw_user_data['address'] = raw_user_data['address'].str.replace('\n', ', ')
        string_cols = list(raw_user_data[['first_name', 'last_name', 'company', 'email_address',
                             'country', 'country_code', 'user_uuid', 'address', 'phone_number']])
        raw_user_data[string_cols] = raw_user_data[string_cols].astype('string')
        # Casting date columns
        # using pd.to_datetime to convert date formats, errors return NaT
        raw_user_data['date_of_birth'] = pd.to_datetime(raw_user_data['date_of_birth'], infer_datetime_format=True, errors='coerce')
        raw_user_data['join_date'] = pd.to_datetime(raw_user_data['join_date'], infer_datetime_format=True, errors='coerce')
        # removing rows where date_of_birth or join_date are null
        null_dobs = raw_user_data['date_of_birth'].isnull()
        null_join_dates = raw_user_data['join_date'].isnull()
        problem_dates = raw_user_data[(null_dobs) | (null_join_dates)]
        raw_user_data = raw_user_data.drop(problem_dates.index)
        # Resets index column on dataframe and returns to main.py
        raw_user_data.reset_index(drop = True, inplace=True)
        return raw_user_data
    
    def clean_card_data(self, raw_card_data):
        '''
        This function takes in raw card data and cleans it.

        Columns are converted to their intended data types and null values are identified and removed.

        Args:
            raw_card_data: a pandas dataframe containing the raw card data to be cleaned.
        
        Returns:
            raw_card_data: a pandas dataframe which as now been cleaned. Will be reassigned as clean_card_data in main.py
        '''
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
        '''
        This function takes in raw store data and cleans it.

        Columns are converted to their intended data types and null values are identified and removed.
        A regex expression is used to identify erroneous data in some columns.

        Args:
            raw_store_data: a pandas dataframe containing the raw store data to be cleaned.
        
        Returns:
            raw_store_data: a pandas dataframe which as now been cleaned. Will be reassigned as clean_store_data in main.py
        '''
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
        '''
        This function takes in raw product data and cleans the 'weight' column.

        A nested function, convert_to_kg, is defined. This function converts the weights of each product into kg.

        The nested convert_to_kg function is applied to all rows in the 'weight' column of the product data.
        After this, any null values are dropped. The product data dataframe is then passed to the clean_product_data function.
        For convert_to_kg to work, any null values are converted to a string that won't be read as a weight unit, 'NaN.
        Once convert_to_kg is applied, these NaN strings are replaced with np.NaN and then dropped.

        Args:
            raw product_data: a pandas dataframe containing the raw store data to be cleaned.
        
        Returns:
            raw product_data: a pandas dataframe which as now been cleaned. Will be reassigned as clean_product_data in main.py
        '''
        def convert_to_kg(weight_col):
            '''
            This function uses a regex expression to convert the weight of the product to kg.

            The regex expression searches for 3 groups: a quantity, a value and a unit (kg, g, ml, oz).
            An if/else block is used to convert the value (depending on its unit), then
            multiplied by the quantity to get the weight of the product in kg.

            If a quantity value is not found, then the nested if/else block is used. This
            block converts the value depending on the unit. The else clause catches any
            weight entries which do not match the regex expression, such as random text
            or a 'NaN' string that replaced any null values from the
            convert_product_weights function.

            Args:
                weight_col: the 'weight' column of the productt data dataframe.
        
            Returns:
                total_value: the weight value once it has been converted into kg.
            '''
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
                    total_value, unit = unit_match.groups()
                    total_value = float(total_value)

                    if unit == 'kg':
                        return total_value
                    elif unit == 'g' or unit == 'ml':
                        return total_value / 1000
                    elif unit == 'oz':
                        return total_value * 0.0283495
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
        '''
        This function is responsible for cleaning the raw product data.

        Columns of string data are cast into a string datatype, then the unique product categories
        are found to identify any typos or erroneous category data. No errors were found, indicating
        this part of the dataframe is fine.

        The price column is then stripped of '£' symbols and converted to a numerical datatype, then dates
        are converted to datetime64. No erroneous data was identified, so the index gets reset and the cleaned
        data returned to main.py for upload to the PostgreSQL database.

        Args:
            raw_product_data: pandas dataframe of the product data after the weight column has been cleaned
                              by convert_product_weights.
        Returns:
            raw_product_data: pandas dataframe of the product data wihch has now been cleaned. Will be reassigned
                              as clean_product_data in main.py
        '''
        string_cols = list(raw_product_data[['product_name', 'category', 'EAN', 'uuid', 'removed', 'product_code']])
        raw_product_data[string_cols] = raw_product_data[string_cols].astype('string')
        print(f'\nAvailable categories are:\n{raw_product_data["category"].unique()}\n\nItem availability statuses are:\n{raw_product_data["removed"].unique()}\n\n')
        
        raw_product_data['product_price'] = raw_product_data['product_price'].str.lstrip('£')
        raw_product_data['product_price'] = pd.to_numeric(raw_product_data['product_price'])
        # Converts date_added column to datetime64. Earliest and latests dates seem sensible, so it
        # looks like the date formats were interpreted correctly. No errors or Null values so this
        # column doesn't seem to require cleaning.
        raw_product_data['date_added'] = pd.to_datetime(raw_product_data['date_added'], format='mixed')

        raw_product_data.reset_index(drop = True, inplace=True)
        return raw_product_data

    def clean_orders_data(self, raw_orders_table):
        '''
        This function cleans the orders table data before it is uploaded to the PostgreSQL database.

        First, 'level_0', 'first_name' and 'last_name' columns are dropped as they seem superfluous.
        No erroneous data was identified, so the index column is reset and the cleaned data is returned
        to main.py.

        Args:
            raw_orders_table: pandas dataframe of orders table data to be cleaned.
        Returns:
            raw_orders_table: pandas dataframe of orders table data which has now been cleaned.
                              It will be reassigned to clean_orders_table in main.py
        '''
        raw_orders_table = raw_orders_table.drop(['level_0', 'first_name', 'last_name', '1'], axis = 1)
        raw_orders_table = raw_orders_table.set_index('index', drop=True)
        raw_orders_table.reset_index(drop = True, inplace=True)
        return raw_orders_table

    def clean_date_events(self, raw_date_events):
        '''
        This function cleans the data events data before it is uploaded to the PostgreSQL database.

        This is done by casting the 'timestamp' column to datetime64 using a H:M:S. Errors
        are coerced to identify any erroneous data. The .dt.time function is used to extract the timestamp
        and remove the Unix Epoch date which would otherwise be added into the column. As a side effect of
        using .dt.time, the datatype of the 'timestamp' column reverts to object.

        The erroneous data is dropped from the table (after being visually checked). The same methodology is applied to the 'month,'
        'year,' and 'day' columns.

        Unique time periods are printed to identify typos and erroneous data. Nothing unusual was found.
        
        Args:
            raw_date_events: pandas dataframe of date events to be cleaned.
        
        Returns:
            raw_date_events: pandas dataframe of date events which has been cleaned. Will be
                             reassigned to clean_date_events in main.py
        '''
        raw_date_events['timestamp'] = pd.to_datetime(raw_date_events['timestamp'], format='%H:%M:%S', errors='coerce').dt.time
        null_timestamps_mask = raw_date_events['timestamp'].isnull()
        null_timestamps = raw_date_events[null_timestamps_mask]
        raw_date_events = raw_date_events.drop(null_timestamps.index)

        raw_date_events['month'] = pd.to_datetime(raw_date_events['month'], format='%m').dt.month
        raw_date_events['year'] = pd.to_datetime(raw_date_events['year'], format='%Y').dt.year
        raw_date_events['day'] = pd.to_datetime(raw_date_events['day'], format='%d').dt.day
        raw_date_events['time_period'].unique()
       
        raw_date_events.reset_index(drop = True, inplace=True)
        return raw_date_events