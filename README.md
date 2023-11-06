# multinational-retail-data-centralisation

## Project Summary
This project aims to centralise data from multiple sources into a singular PostregSQL database.

The data relates to a retail business and contains information on products, customers, card details, stores and orders.

The project extracts the data from multiple difference sources, including:
- pdf file
- json file
- AWS S3 bucket
- AWS API

The project taught and consolidated skills and methods required to extract data from the aforementioned sources, confert the data to pandas dataframe for cleaning, and upload the dataframes as PostgreSQL tables. In addition, it consolidated knowledge of Python PEP code formatting guidelines.

## Dependencies
-[boto3](https://aws.amazon.com/sdk-for-python/) - Amazon Web Services (AWS) SDK for Python.
-[dateutil](https://pypi.org/project/python-dateutil/) - Provides extensions to the standard datetime module.
-[numpy](https://numpy.org/) - Adds support for large, multi-dimensional arrays and matrices.
-[pandas](https://pandas.pydata.org/) - Data analysis and manipulation tool for Python.
-[psycopg2](https://www.psycopg.org/docs/) - PostgreSQL adapter for the Python programming language.
-[requests](https://pypi.org/project/requests/) - HTTP library for Python, allowing you to send HTTP/1.1 requests.
-[sqlalchemy](https://www.sqlalchemy.org/) - SQL toolkit and Object-Relational Mapping (ORM) library for Python.
-[tabula](https://tabula-py.readthedocs.io/) - Extracts tables from PDFs into pandas DataFrames.
-[yaml](https://pyyaml.org/wiki/PyYAMLDocumentation) - YAML parser and emitter for Python.


