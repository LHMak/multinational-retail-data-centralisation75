# Defining data extractor class
from database_utils import DatabaseConnector
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
import pandas as pd

class DataExtractor:
    # Takes list of tables and engine as argument, returns table contents as a Pandas Dataframe
    def read_rds_table(self, table_list, engine):
        chosen_table = input(f"Select a table to view from:\n{table_list}: ")
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {chosen_table}"))
            df_result = pd.DataFrame(result)
            print(df_result.head(10))
            return df_result

