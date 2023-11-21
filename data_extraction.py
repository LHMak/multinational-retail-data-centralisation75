# Defining data extractor class
from sqlalchemy import text
from sqlalchemy.engine import Engine
import pandas as pd
import tabula

class DataExtractor:
    # Takes engine and table as arguments, returns table contents as a Pandas Dataframe
    def read_rds_table(self, engine: Engine, chosen_table):
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {chosen_table}"))
            df_result = pd.DataFrame(result)
            # print(df_result.head(10))
            return df_result
    def retrieve_pdf_data(self, link):
        # Read pdf into list of DataFrame
        pdf_data = tabula.read_pdf(link, pages='all')
        return pdf_data