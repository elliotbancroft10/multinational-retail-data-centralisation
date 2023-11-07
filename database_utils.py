import yaml
import psycopg2

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

class DatabaseConnector:
    """A class containing methods to perform and execute actions on AWS and Postres databases."""
    def _read_db_creds(self):
        """Reads and returns the RDS database credentials from a yaml file."""
        creds_path = "C:/Users/Elliot/pyproj/AICore/mrdc/multinational-retail-data-centralisation/db_creds.yaml"
        with open(creds_path, 'r') as f:
            creds_dict = yaml.load(f, Loader=yaml.FullLoader)
            return creds_dict
    
    def _init_db_engine(self, creds_dict):
        """Uses RDS database credentials to return an SQLAlchemy engine object."""
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds_dict['RDS_HOST']
        USER = creds_dict['RDS_USER']
        PASSWORD = creds_dict['RDS_PASSWORD']
        DATABASE = creds_dict['RDS_DATABASE']
        PORT = creds_dict['RDS_PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
    
    def _list_db_tables(self, engine):
        """Creates a connection to the RDS database and returns the public table names."""
        with engine.connect() as connection:
            select_query = text("""
                                SELECT table_name
                                FROM information_schema.tables
                                WHERE table_schema = 'public';"""
                                )
            results = connection.execute(select_query)
            return results

    def upload_to_db(self, df, upload_table_name='orders_table'):
        """Uploads a dataframe to a PostreSQL table."""
        # Database connection parameters
        db_params = {
            "host": "localhost",
            "database": "sales_data",
            "user": "postgres",
            "password": "bancr0ft"
        }      
        try:
            # Establish a connection to the PostgreSQL database
            engine = create_engine(
                f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}/{db_params["database"]}'
                )
            # Use pandas to upload the DataFrame to the database
            df.to_sql(upload_table_name, engine, if_exists='replace', index=False)
            print("DataFrame uploaded successfully!")

        except Exception as e:
            # Raise an error if dataframe not uploaded successfully
            print("Error:", e)


