import pandas as pd
import tabula

from sqlalchemy import text

class DataExtractor:
    """
    A utility class that contains methods which extract data from CSV files, an API and an S3 bucket.
    """
    def read_rds_table(self, DC, table_name='legacy_users'):
        """Reads a specified table from the RDS database."""
        # Get database credentials
        creds_dict = DC._read_db_creds(self)
        # Extract data from the specified table and return as a pandas DataFrame
        with DC._init_db_engine(self, creds_dict).connect() as connection:
            select_query = text(f"""
                                SELECT *
                                FROM {table_name}
                                """
                                )
            data = connection.execute(select_query)            
        df = pd.DataFrame(data)
        return df
    
    def retrieve_pdf_data(self, path='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'):
        """Read data from a pdf and return it as a pandas dataframe."""
        df_list = tabula.read_pdf(path, pages='all')
        # concatenate the dataframes from different pages and make the index continuous
        df = pd.concat(df_list).reset_index(drop=True)
        return df

DE = DataExtractor()
DE.retrieve_pdf_data()

"""
DE = DataExtractor()
Clean = DataCleaning()
Connect = DC()
df = DE.read_rds_table(DC)
df = Clean.clean_user_data(df)
Connect.upload_to_db(df)
"""
