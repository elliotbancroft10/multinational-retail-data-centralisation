from dateutil.parser import parse

import pandas as pd
import numpy as np

class DataCleaning:
    """A class containing methods for cleaning user and card data."""

    def clean_user_data(self, df):
        """A method for cleaning the legacy_users RDS table."""    
        # Drop all rows with null values since dataset is 
        # large enough not to be impacted significatly
        df.dropna(inplace=True)
        
        # Deal with dates
        date_cols = ['date_of_birth', 'join_date']
        def _parse_date(value):
            try:
                return parse(value)
            except (ValueError, TypeError):
                return pd.NaT

        for col in date_cols:
            df[col] = df[col].apply(_parse_date)
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
        
        # Remove rows containing NaT values
        df.dropna(subset=date_cols, inplace=True)

        # Deal with email addresses
        email_regex = '^([a-zA-Z0-9_\-\.]+)@([a-zA-Z0-9_\-\.]+)\.([a-zA-Z]{2,5})$'
        df.loc[~df['email_address'].str.match(email_regex), 'email_address'] = np.nan

        # Drop rows conataining NaN values due to incorrect email addresses
        df.dropna(inplace=True)

        # Deal with GGB country codes
        df['country_code'] = df['country_code'].str.replace('GGB', 'GB')

        # Deal with phone numbers
        uk_regex = '((\+44\s?\(0\)\s?\d{2,4})|(\+44\s?(01|02|03|07|08)\d{2,3})|(\+44\s?(1|2|3|7|8)\d{2,3})|(\(\+44\)\s?\d{3,4})|(\(\d{5}\))|((01|02|03|07|08)\d{2,3})|(\d{5}))(\s|-|.)(((\d{3,4})(\s|-)(\d{3,4}))|((\d{6,7})))'
        germany_regex = '(\(?([\d \-\)\–\+\/\(]+){6,}\)?([ .\-–\/]?)([\d]+))'
        us_regex = '^([a-zA-Z,#/ \.\(\)\-\+\*]*[2-9])([a-zA-Z,#/ \.\(\)\-\+\*]*[0-9]){2}([a-zA-Z,#/ \.\(\)\-\+\*]*[2-9])([a-zA-Z,#/ \.\(\)\-\+\*]*[0-9]){6}[0-9a-zA-Z,#/ \.\(\)\-\+\*]*$'

        if (df['country'] == 'United Kingdom').any():
            df.loc[~df['phone_number'].str.match(uk_regex), 'phone_number'] = np.nan 
        elif (df['country'] == 'Germany').any():
            df.loc[~df['phone_number'].str.match(germany_regex), 'phone_number'] = np.nan 
        else:
            df.loc[~df['phone_number'].str.match(us_regex), 'phone_number'] = np.nan 

        # Drop any exact duplicate rows.
        df.drop_duplicates()
        return df
    
    def clean_card_data(self, df):
        """A method for cleaning the card details table read from a pdf."""
        df.dropna(inplace=True)
        return df
    
    def called_clean_store_data(self, df):
        """Cleans data from retrieve_stores_data method."""
        df.dropna(inplace=True)
        return df

    def convert_product_weights(self, df):
        """Cleans products table weight column"""
        df.dropna(inplace=True)
        # Extract numeric part
        df['weight_numeric'] = df['weight'].str.extract('(\d+\.\d+|\d+)').astype(float)  
        # Extract unit part
        df['weight_unit'] = df['weight'].str.extract('([a-zA-Z]+)')  
        df['weight_numeric'] = np.where(df['weight_unit'] == 'ml', df['weight_numeric'], 
                                        np.where(df['weight_unit'] == 'g', df['weight_numeric'] / 1000, 
                                                 df['weight_numeric']))
        df['weight'] = df['weight_numeric'].round(2)
        df = df.drop(columns=['weight_numeric', 'weight_unit'])  # Corrected typo here
        return df
        
    def clean_products_data(self, df):
        """Cleans data from products table dataframe."""
        #TODO deal with 0 weights
        df.dropna(inplace=True)
        return df
    
    def clean_orders_data(self, df):
        """Cleans data from orders table by dropping incomplete columns."""
        df = df.drop(columns=['first_name', 'last_name', '1'])
        return df
    
    def clean_date_times(self, df):
        """Cleans data from the date_times table."""
        df['month'] = pd.to_numeric(df['month'], errors='coerce')
        # Drop rows where 'months' column is NaN (non-integer values)
        df.dropna(inplace=True)
        return df


        
                 