# Multinational Retail Data Centralisation project
Collates, cleans and centralises store data into a single location.


## Project Brief
> You work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.

> Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. You will then query the database to get up-to-date metrics for the business.

## Project Description

I have undertaken this project as part of the AiCore Cloud Engineering pathway. The aim of this project was to collate, clean and centralise the sales data from a fictional multinational company. 

The company sells goods across the globe and their sales data was spread across many different data sources. This meant the data was not easily accessible or analysable.

In order to remedy this issue, this project did the following:

1. Extracted all data from the various sources such as: AWS S3 buckets, AWS cloud databases and APIs hosted on AWS. The data also took many forms such as: PDF, JSON and CSV files. This required me to tailor my extraction approach for each datasource. For example, user and order data was stored in a database, while product and payment data was stored in an S3 bucket.
1. Cleaned the raw data, looking for inconsistent formatting (particularly with dates and times), typos and null values. To clean the data I mainly used the Pandas Python library. I also used the re and numpy libraries.
1. Uploaded the cleaned data to a local PostgreSQL database, allowing for easy querying and analysis of the data. I used PGAdmin as a management tool for the PostgreSQL database.
1. Queries the PostgreSQL database to answer a set of mock questions from business stakeholders. The `.sql` files for these queries can be found here: [Milestone_4_Queries.zip](Milestone_4_Queries.zip)

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
**Prerequisites:** 
- A local SQL database and corresponding server (pgAdmin4 in my case)
- Two YAML files containing database credentials:
  - `db_creds.yaml` - File containing the credentials to connect to the source AWS database
  - `sales_data_creds.yaml` - File containing the credentials to connect to the targest PostgreSQL database
  - This screenshot shows the required template the credential YAML files must follow:
   
    ![TEMPLATE_creds](https://github.com/LHMak/multinational-retail-data-centralisation75/assets/147920042/79969bed-321e-4662-8ee6-9d5fa8102478)

- `api_key_header.yaml` - File containing an X-API-key to be used in the headers of a get request for listing the number of stores in the business.
  - This screenshot shows the required format of YAML file containing the API key: 

    ![TEMPLATE_x_api_key](https://github.com/LHMak/multinational-retail-data-centralisation75/assets/147920042/e8a3db13-eacc-4b07-bc30-eb435da59b0a)
    
- **[TODO] add imported Python libraries and modules**

Once these prerequisites are satisfied, just run main.py. Once this script has terminated, the PostgreSQL database will hold the following tables: dim_card_details, dim_date_times, dim_products, dim_store_details, dim_users and orders_table.

## Project write-up
In this section, I explain how I completed this project, the methods and software I used and challenges I faced.

The project was split up into 4 stages, or 'Milestones.' Each Milestone is explained below.

### Milestone 1: Set up the environment
This project was split up into 4 milestones. This first milestone's goal was to set up this GitHub repository so I could save my code and track any changes.

One feature of Github I used extensively through this project was branching. Every time I needed to add a new feature (such as extracting data from a new data source) I would check out to a new branch. This would allow me write new code and test its functionality without affecting my main version of the project. Once I was happy with my work, I would merge the feature branch with the main branch.

In total, I utilised 13 branches:
<img width="968" alt="image" src="https://github.com/LHMak/multinational-retail-data-centralisation75/assets/147920042/ebb6fe96-2c4e-4d71-83de-a403204e519c">

### Milestone 2: Extract and clean the data from the data sources
#### Task 1: Set up a new database to store the data
With the GitHub repo set up, it was time to move onto Milestone 2. The goal of this milestone was to extract all of the data from each data source, clean it and then store it in a new database.

I began by creating the a PostgreSQL database using pgAdmin4. This database would act as the destination for the data. To do this, I right-clicked on Databases under the default PostgreSQL server in pgAdmin4. From there, I selected `Create > Database...` 

<img width="873" alt="m2 1 create db3" src="https://github.com/LHMak/multinational-retail-data-centralisation75/assets/147920042/f9bece57-d3c1-4888-9ac9-b78cbd83e0f7">
 
 I named the database 'sales_data' and then moved on to writing my first bit of Python code for this project.
 
#### Task 2: Initialise the three project Classes
To achieve the goal of centralising all of the data, I would need to:

- Retrieve the data from its source (e.g. Amazon S3 bucket, PDF file, API).
- Clean the data (e.g. remove non-numeric characters from card numbers, remove any corrupted rows, convert dates to a specific format).
- Upload the cleaned data to the new database, sales_data.

Therefore, I decided that these interactions would be best separated into 3 custom Python classes: 'DataExtractor', 'DatabaseConnector', 'DataCleaning'. I could then create methods for each class to handle any funcitonality I would need, such as extracting data from a PDF file (DataExtractor) or stripping card numbers of any non-numeric characters(DataCleaning).

Rather than having one

### Milestone 4: Querying the data.
In this milestone, the goal was to answer a set of business questions using the newly created database.

The questions were:

## File Structure of Project
- **.gitignore:** Contains list of files which are not tracked by Git. In particular for this project, database credentials are included in the gitignore file and so, are not uploaded to this Github repository.
- **LICENSE:** The License (MIT) file for this project.
- **README.md:** The README markdown file for this project. Contains information about the project's purpose, tools used, file structure, etc.
- **main.py:** This Python script serves are the main controller of the project processes. It works by calling functions from the DatabseConnector, DataExtractor and DataCleaning classes described in the following 3 Python scripts. By using a main.py script, the data that has been extracted by the DataExtractor can be passed to the DataCleaning class; then to the DatabaseConnector to upload to the centralised PostgreSQL database.
- **database_utils.py:** This script introduces the DatabaseConnector class, which is responsible for reading database credentials (in the form of a .YAML file); initialising an SQLAlchemy/psycopg2 engine to manage the connection to a database; listing the tables in a databse to allow selection of data for extraction and finally, uploading cleaned data to the target PostgreSQL database.
- **data_extraction.py:** This script introduces the DataExtractor class, which is responsible for extracting data from a source and generating a pandas dataframe from it if cleaning is required. This class contains 5 extraction functions, which extract from the following source types: RDS tables, PDF documents, APIs, JSON and CSV files.
- **data_cleaning.py:** This script introduces the DataCleaning class, which is responsible for taking in raw data and cleaning it. The data cleaning methods are different for each data source- but typically, null and erroneous entries are identified and removed, typos are corrected and columns are cast to their intended datatypes.
- **Milestone_4_Queries.zip:** This `.zip` folder contains 10 `.sql` files. Each file contains a query to answer one of the questions from a business stakeholder.

## License Information
This project is licensed under the terms of the MIT license.
