import pandas as pd
import tabula
import requests
import os
import boto3

from sqlalchemy import text

class DataExtractor:
    """
    A utility class that contains methods which extract data from an RDS table, CSV files, an API,
    an S3 bucket, a pdf file and a json file.
    """
    def read_rds_table(self, Connector, table_name='orders_table'):
        """Reads a specified table from the RDS database."""
        # Get database credentials
        creds_dict = Connector._read_db_creds()
        # Extract data from the specified table and return as a pandas DataFrame
        # Initiate connection to RDS database
        with Connector._init_db_engine(creds_dict).connect() as connection:
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
        # Concatenate the dataframes from different pages and make the index column continuous
        df = pd.concat(df_list).reset_index(drop=True)
        return df
    
    def list_number_of_stores(
            self, 
            endpt='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',
            headers={'x-api-key': None}):
        """Makes an API request and returns the number of retail stores."""
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
        """Makes an API request and returns a dictionary of details for each retail store.
        The dictionary is then converted to a dataframe and returned."""
        store_data = {}
        api_key = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
        headers['x-api-key'] = api_key
        # Retrieve number of stores from list_number_of_stores method
        total_store_number = self.list_number_of_stores()['number_stores']
        # Loop through stores and append details dictionaries by incrementing API endpoint
        for store_number in range(total_store_number):
            try:
                store_endpt = endpt.format(store_number=store_number)
                response = requests.get(store_endpt, headers=headers)
                # Raise an exception for 4xx and 5xx status codes
                response.raise_for_status()  
                store_info = response.json()
                store_data[store_number] = store_info            
            except requests.exceptions.RequestException as e:
                print(f"Error for store {store_number}: {e}")
                store_data[store_number] = None         
        df = pd.DataFrame.from_dict(store_data, orient='index')
        return df

    def extract_from_s3(self, s3_path='s3://data-handling-public/products.csv'):
        """Downloads and extracts data from the products.csv file from an s3 bucket."""
        # Initialize the S3 client
        boto3.setup_default_session(profile_name='Elliot') 
        # S3 bucket and file details
        bucket_name = 'data-handling-public'
        # Download the file from S3
        s3 = boto3.client('s3')
        s3.download_file(bucket_name, s3_path, 'products.csv')
        df = pd.read_csv('products.csv')
        return df 
    
    def retrieve_json_datetime(self, path='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'):
        """Returns a dataframe of dates from a json file."""
        response = requests.get(path)
        # Check if the request was successful
        if response.status_code == 200:
            # Parse JSON content into a DataFrame
            json_data = response.json()
            df = pd.DataFrame(json_data)
            return df
        else:
            print("Failed to retrieve JSON data from the URL.")