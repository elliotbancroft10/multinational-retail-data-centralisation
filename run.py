"""
The main file for performing actions on data. Instances of the DatabaseConnector, 
DataExtractor and DataCleaning classes have been created to facilitate connections
to required databases, extraction and cleaning processes using one file. 

Start editing after line 15.
"""

from database_utils import DatabaseConnector 
from data_extraction import DataExtractor 
from data_cleaning import DataCleaning 

Connector = DatabaseConnector()
Extract = DataExtractor()
Clean = DataCleaning()