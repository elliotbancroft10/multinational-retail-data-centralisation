import pandas as pd
import tabula
import requests
import os
import boto3

from sqlalchemy import text

from database_utils import DatabaseConnector as DC
from data_cleaning import DataCleaning

class DataExtractor:
    """
    A utility class that contains methods which extract data from CSV files, an API and an S3 bucket.
    """
    def read_rds_table(self, DC, table_name='orders_table'):
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
    
    def list_number_of_stores(
            self, 
            endpt='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',
            headers={'x-api-key': None}):
        """Makes an API request and returns the number of stores."""
        api_key = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
        headers['x-api-key'] = api_key
        try:
            response = requests.get(endpt, headers=headers)
            # Raise an exception for 4xx and 5xx status codes
            response.raise_for_status()  
            number_of_stores = response.json()
            return number_of_stores
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None      
    
    def retrieve_stores_data(
            self, 
            endpt='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}', 
            headers={'x-api-key': None}):
        """Makes an API request and returns the number of stores."""
        store_data = {}
        api_key = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
        headers['x-api-key'] = api_key
        total_store_number = self.list_number_of_stores()['number_stores']
        for store_number in range(total_store_number):
            try:
                store_endpt = endpt.format(store_number=store_number)
                response = requests.get(store_endpt, headers=headers)
                # Raise an exception for 4xx and 5xx status codes
                response.raise_for_status()  
                store_info = response.json()
                store_data[store_number] = store_info 
                #print(store_info)             
            except requests.exceptions.RequestException as e:
                print(f"Error for store {store_number}: {e}")
                store_data[store_number] = None         
        df = pd.DataFrame.from_dict(store_data, orient='index')
        return df

    def extract_from_s3(self, s3_path='s3://data-handling-public/products.csv'):
        """Downloads and extracts data from the products.csv file from an s3 bucket."""
        # Initialize the S3 client
        """boto3.setup_default_session(profile_name='Elliot')
        
        # S3 bucket and file details
        bucket_name = 'data-handling-public'

        # Download the file from S3
        s3 = boto3.client('s3')
        s3.download_file(bucket_name, s3_path, 'products.csv')"""

        # Read the CSV file into a DataFrame
        df = pd.read_csv('products.csv')

        # Now df contains your data from the S3 bucket
        return df 
    
    def retrieve_json_data(self, path='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'):
        response = requests.get(path)
        # Check if the request was successful
        if response.status_code == 200:
            # Parse JSON content into a DataFrame
            json_data = response.json()
            df = pd.DataFrame(json_data)
            return df
        else:
            print("Failed to retrieve JSON data from the URL.")
            


