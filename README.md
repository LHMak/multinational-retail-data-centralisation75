# Multinational Retail Data Centralisation project
Collates, cleans and centralises store data into a single location.

## Project Description

I have undertaken this project as part of the AiCore Cloud Engineering pathway. The aim of this project was to collate, clean and centralise the sales data from a fictional multinational company. 

The company sells goods across the globe and their sales data was spread across many different data sources. This meant the data was not easily accessible or analysable.

In order to remedy this issue, this project did the following:

1. Extracted all data from the various sources such as: AWS S3 buckets, AWS cloud databases and APIs hosted on AWS. The data also took many forms such as: PDF, JSON and CSV files. This required me to tailor my extraction approach for each datasource. For example, user and order data was stored in a database, while product and payment data was stored in an S3 bucket.
1. Cleaned the raw data, looking for inconsistent formatting (particularly with dates and times), typos and null values. To clean the data I mainly used the Pandas Python library. I also used the re and numpy libraries.
1. Uploaded the cleaned data to a PostgreSQL database, allowing for easy querying and analysis of the data. I used PGAdmin as a management tool for the PostgreSQL database.
1. Queries the PostgreSQL database to answer a set of mock questions from business stakeholders. The queries are shown in 

This project helped me to consolidate everything I have learned throughout the course. I used my knowledge of AWS and APIs to retrieve the data files, my skills with VS Code and Python to write the code to process the data and upload it to the PostgreSQL database- and finally, my knowledge and skills with Relational Databases and SQL to create the database schema, query and analyse the data. On top of this, I made more use of version control and branching with Git more than ever before- particularly when adding in new features.

Because of this, my confidence with with all of these skills has grown substantially. I have also come to appreciate just how much I have learnt in the last ~1.5 months.

## Tools used
- Python
  - Pandas library
  - PandasGUI
  - Regular expression operations (re) module
  - NumPy
  - SQLAlchemy library toolkit
  - TQDM library
  - Tabula Python wrapper
  - Requests library
  - AWS SDK for Python (boto3)
  - OS module
  - YAML library
- VS Code
  - Python extension for VS Code
  - Pylance
- PostgreSQL
  - pgAdmin 4

## Usage Instructions
**Prerequisites:** You will need a db_creds.yaml file, which contains the credentials to connect to the source AWS database, and a sales_data_creds.yaml file, which contains the credentials to connect to the targest PostgreSQL database. Finally, you will need a api_key_header.yaml file which contains an X API key to be used in the headers of a get request for listing the number of stores in the business.

Once these prerequisites are satisfied, just run main.py. Once this script has terminated, the PostgreSQL database will now hold the following tables: dim_card_details, dim_date_times, dim_products, dim_store_details, dim_users and orders_table.

## File Structure of Project
- **.gitignore:** Contains list of files which are not tracked by Git. In particular for this project, database credentials are included in the gitignore file and so, are not uploaded to this Github repository.
- **LICENSE:** The License (MIT) file for this project.
- **REAME.md:** The README markdown file for this project. Contains information about the project's purpose, tools used, file structure, etc.
- **main.py:** This Python script serves are the main controller of the project processes. It works by calling functions from the DatabseConnector, DataExtractor and DataCleaning classes described in the following 3 Python scripts. By using a main.py script, the data that has been extracted by the DataExtractor can be passed to the DataCleaning class; then to the DatabaseConnector to upload to the centralised PostgreSQL database.
- **database_utils.py:** This script introduces the DatabaseConnector class, which is responsible for reading database credentials (in the form of a .YAML file); initialising an SQLAlchemy/psycopg2 engine to manage the connection to a database; listing the tables in a databse to allow selection of data for extraction and finally, uploading cleaned data to the target PostgreSQL database.
- **data_extraction.py:** This script introduces the DataExtractor class, which is responsible for extracting data from a source and generating a pandas dataframe from it if cleaning is required. This class contains 5 extraction functions, which extract from the following source types: RDS tables, PDF documents, APIs, JSON and CSV files.
- **data_cleaning.py:** This script introduces the DataCleaning class, which is responsible for taking in raw data and cleaning it. The data cleaning methods are different for each data source- but typically, null and erroneous entries are identified and removed, typos are corrected and columns are cast to their intended datatypes.
- **Milestone_4_Queries.zip:** This .zip folder contains 10 .sql files. Each file contains a query to answer one of the questions from a business stakeholder.

## License Information
This project is licensed under the terms of the MIT license.
