import pandas as pd
import numpy as np
import uuid
import psycopg2

from dateutil.parser import parse

class DataCleaning:
    """A class containing methods for cleaning different dataframes."""
    def clean_user_data(self, df):
        """A method for cleaning the legacy_users RDS table."""    
        # Deal with dates
        date_cols = ['date_of_birth', 'join_date']
        # Define a function to parse column dates
        def __parse_date(value):
            try:
                return parse(value)
            except (ValueError, TypeError):
                return pd.NaT
        # Parse column data and convert to datetime
        for col in date_cols:
            df[col] = df[col].apply(__parse_date)
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
        # Deal with email addresses
        email_regex = '^([a-zA-Z0-9_\-\.]+)@([a-zA-Z0-9_\-\.]+)\.([a-zA-Z]{2,5})$'
        df.loc[~df['email_address'].str.match(email_regex), 'email_address'] = np.nan
        # Deal with GGB country codes
        df['country_code'] = df['country_code'].str.replace('GGB', 'GB')
        # Deal with phone numbers
        uk_regex = '((\+44\s?\(0\)\s?\d{2,4})|(\+44\s?(01|02|03|07|08)\d{2,3})|(\+44\s?(1|2|3|7|8)\d{2,3})|(\(\+44\)\s?\d{3,4})|(\(\d{5}\))|((01|02|03|07|08)\d{2,3})|(\d{5}))(\s|-|.)(((\d{3,4})(\s|-)(\d{3,4}))|((\d{6,7})))'
        germany_regex = '(\(?([\d \-\)\–\+\/\(]+){6,}\)?([ .\-–\/]?)([\d]+))'
        us_regex = '^([a-zA-Z,#/ \.\(\)\-\+\*]*[2-9])([a-zA-Z,#/ \.\(\)\-\+\*]*[0-9])\\{2\}([a-zA-Z,#/ \.\(\)\-\+\*]*[2-9])([a-zA-Z,#/ \.\(\)\-\+\*]*[0-9]){6}[0-9a-zA-Z,#/ \.\(\)\-\+\*]*$'
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
        # Drop any rows containing text in the card number column
        df = df[pd.to_numeric(df['card_number'], errors='coerce').notna()]
        return df
    
    def clean_store_data(self, df):
        """Cleans data from retrieve_stores_data method."""
        # Drop the lat column
        df.drop('lat', axis=1, inplace=True)
        # Convert any non-numerical latitude and staff number values to NULL
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
        return df

    def convert_product_weights(self, df):
        """Cleans products table weight column"""
        # Extract numeric part using a regex
        df['weight_numeric'] = df['weight'].str.extract('(\d+\.\d+|\d+)').astype(float)  
        # Extract unit, convert ml values to g, then divide all g values by 1000
        df['weight_unit'] = df['weight'].str.extract('([a-zA-Z]+)')  
        df['weight_numeric'] = np.where(df['weight_unit'] == 'ml', df['weight_numeric'], 
                                        np.where(df['weight_unit'] == 'g', df['weight_numeric'] / 1000, 
                                                 df['weight_numeric']))
        df['weight'] = df['weight_numeric'].round(2)
        # Drop weight_numeric and weight_unit columns so they aren't uploaded
        df = df.drop(columns=['weight_numeric', 'weight_unit']) 
        return df
        
    def clean_products_data(self, df):
        """Cleans data from products table dataframe."""
        # Replace 0 weight values with NULL
        df['weight'] = df['weight'].replace(0, np.nan)
        # Remove £ signs from product_price column and convert any alphabetical values to NULL
        df['product_price'] = df['product_price'].str.replace('£', '')
        df['product_price'] = pd.to_numeric(df['product_price'], errors='coerce')
        return df
    
    def clean_orders_data(self, df):
        """Cleans data from orders table by dropping incomplete columns."""
        # Drop inclomplete columns from the table as instricted in task
        df = df.drop(columns=['first_name', 'last_name', '1'])
        return df
    
    def clean_date_times(self, df):
        """Cleans data from the date_times table."""
        # Convert all non-numeric month values where possible, and NULL if not possible
        month_mapping = {
            'jan': 1, 'january': 1,
            'feb': 2, 'february': 2,
            'mar': 3, 'march': 3,
            'apr': 4, 'april': 4,
            'may': 5,
            'jun' : 6, 'june': 6,
            'jul' : 7, 'july': 7,
            'aug': 8, 'august': 8,
            'sep': 9, 'september': 9,
            'oct': 10, 'october': 10,
            'nov': 11, 'november': 11,
            'dec': 12, 'december' : 12
        }
        # Use str.lower to eliminate case sensitivity and convert any remaining
        # non-numeric values to NULL
        df['month'] = df['month'].str.lower().map(month_mapping)
        df['month'] = pd.to_numeric(df['month'], errors='coerce')
        return df      