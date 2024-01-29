# Multinational Retail Data Centralisation project
Collates, cleans and centralises store data into a single location.


## Project Brief
> You work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.

> Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. You will then query the database to get up-to-date metrics for the company.

## Project Description

I undertook this project as part of the AiCore Cloud Engineering pathway. The aim was to collate, clean and centralise the sales data from a fictional multinational retail company

_As a disclaimer- the data used for this project was fictional, so no real customer/company data is being leaked!_ 

The company sells goods across the globe and their sales data was spread across many different data sources. This meant the data was not easily accessible or analysable. The various data sources were:

- An SQL database hosted on AWS RDS containing:
  - User data: Table containing historical data of customers.
  - Orders table: Table in which each column is a UUID from one of the other data sources. It acts as the centre of the star-based schema made in the the destination PostgreSQL database.

- An AWS S3 bucket:
  - Card details: PDF document containing customer payment card details.
  - Product data: CSV file containing information about goods the company sells.
  - Date events data: JSON object containing details of each sale the company has made.

- An API which had two GET methods:
  - Number of stores: JSON object recording the number of stores in the business.
  - Store details: JSON object containing details of each store in the business (e.g. address, staff numbers, store code).


In order to centralise the various pieces of data into once location, this project did the following:

1. Extracted all data from the various sources, tailoring my extraction approach for each data source. For example, user and order data was stored in a database, while product and payment data was stored in an S3 bucket.
1. Cleaned the raw data, looking for inconsistent formatting (particularly with dates and times), typos and null values. To clean the data I mainly used the Pandas Python library. I also used the re and NumPy libraries.
1. Uploaded the cleaned data to a local PostgreSQL database, allowing for easy querying and analysis of the data. I used pgAdmin4 as a management tool for the PostgreSQL database.
1. Queries the PostgreSQL database to answer a set of questions from business stakeholders. The `.sql` files for these queries can be found here: [Milestone_4_Queries.zip](Milestone_4_Queries.zip)

This project helped me to consolidate everything I have learned throughout the course. I used my knowledge of AWS and APIs to retrieve the data files, my skills with VS Code and Python to write the code to process the data and upload it to the PostgreSQL database- and finally, my knowledge and skills with Relational Databases and SQL to create the database schema, query and analyse the data. On top of this, I made more use of version control and branching with Git more than ever before- particularly when adding in new features.

Because of this, my confidence with with all of these skills has grown substantially. I have also come to appreciate just how much I have learnt in the last ~1.5 months.

## Tools used
- Python (with following modules & libraries):
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
- VS Code (with following extensions:)
  - Python extension for VS Code
  - Pylance
- PostgreSQL
  - pgAdmin 4
 
## Usage Instructions
**Prerequisites:** 
- A local SQL database and corresponding server (I used a PostgreSQL database and pgAdmin4 to facilitate this).
- Two YAML files containing database credentials:
  - `db_creds.yaml` - File containing the credentials to connect to the source AWS database
  - `sales_data_creds.yaml` - File containing the credentials to connect to the target PostgreSQL database
  - This screenshot shows the required template the credential YAML files must follow:
   
    ![TEMPLATE_creds](https://github.com/LHMak/multinational-retail-data-centralisation75/assets/147920042/79969bed-321e-4662-8ee6-9d5fa8102478)

- `api_key_header.yaml` - File containing an X-API-key to be used in the headers of a get request for listing the number of stores in the company.
  - This screenshot shows the required format of YAML file containing the API key: 

    ![TEMPLATE_x_api_key](https://github.com/LHMak/multinational-retail-data-centralisation75/assets/147920042/e8a3db13-eacc-4b07-bc30-eb435da59b0a)
    
- You will require the following Python modules and libraries:
  - YAML
  - SQLAlchemy
  - pandas
  - Tabula
  - Requests
  - boto3
  - OS
  - re
  - NumPy

Once these prerequisites are satisfied, just run main.py and when the script has terminated, your local database should hold the following tables: dim_card_details, dim_date_times, dim_products, dim_store_details, dim_users and orders_table.

## Project write-up
In this section, I explain how I completed this project, the methods and software I used and challenges I faced.

The project was split up into 4 stages, or 'Milestones.' Each Milestone is explained below.

### Milestone 1: Set up the environment
This project was split up into 4 milestones. This first milestone's goal was to set up this GitHub repository so I could save my code and track any changes.

One feature of Github I used extensively through this project was branching. Every time I needed to add a new feature (such as extracting data from a new data source) I would check out to a new branch. This would allow me write new code and test its functionality without affecting my main version of the project. Once I was happy with my work, I would merge the feature branch with the main branch.

In total, I utilised 13 branches:
<img width="968" alt="image" src="https://github.com/LHMak/multinational-retail-data-centralisation75/assets/147920042/ebb6fe96-2c4e-4d71-83de-a403204e519c">

### Milestone 2: Extract and clean the data from the data sources
#### Setting up the destination database
With the GitHub repo set up, it was time to move onto Milestone 2. The goal of this milestone was to extract all of the data from each data source, clean it and then store it in a new database.

I began by creating the a PostgreSQL database using pgAdmin4. To do this, I right-clicked on 'Databases' under the default PostgreSQL server in pgAdmin4. From there, I selected `Create > Database...` 

<img width="873" alt="m2 1 create db3" src="https://github.com/LHMak/multinational-retail-data-centralisation75/assets/147920042/f9bece57-d3c1-4888-9ac9-b78cbd83e0f7">
 
 I named the database 'sales_data' and then moved on writing the code to extract all the data from each data source.
 
#### Extracting, cleaning and updating the data
There would be 3 main interactions with the data for this project:

- Connecting to databases (the AWS RDS and the destination sales_data database).
- Extracting the data (e.g. from a PDF or CSV).
- Cleaning the data (e.g. converting dates to consistent format, removing erroneous entries).

Therefore, to keep my code tidy and readable, I utilised the Object Oriented Programming (OOP) principle of encapsulation, separating these interactions into 3 classes (`DataExtractor`, `DatabaseConnector` and `DataCleaning`). Each class contained bespoke functions for each data source and was contained in its own Python script (`data_cleaning.py`, `data_extraction.py` and `database_ults.py`)

I then created the `main.py` script which would act as the orchestrator of events in this pipeline. The script follow this typical flow:

- Takes in database credentials, Amazon S3 bucket link or API endpoint (depending on where the data is stored) and extracts the raw data.
  - If the data is stored in an AWS database, the credentials (in `db_creds.yaml`) are supplied to the `DatabaseConnector` class and a connection is made. The `DataExtractor` class is then used to retrieve contents of the table as a pandas dataframe.
  - If the data is stored as an S3 bucket object, its link is supplied to `DataExtractor` and the file is processed and returned as a pandas dataframe.
  - If the data is retrieved via an API GET request, the endpoint is supplied to `DataExtractor` and a GET request is made. The response is returned as a JSON object and converted to a pandas dataframe if required.
- Cleans the raw data, removing any erroneous data using the `DataCleaning` class
- Uploads the cleaned data to the destination sales_data database by supplying the credentials (in `sales_data_creds.yaml`) to the `DatabaseConnector` class.

Once the code had been written and tested, I ran `main.py` and could see in pgAdmin4 that the database now contained the following tables:

<img width="148" alt="image" src="https://github.com/LHMak/multinational-retail-data-centralisation75/assets/147920042/890d1ed3-69ad-4951-bba3-05e9ce85a634">

### Milestone 3: Create the database schema
With the cleaned data uploaded to the new database, I could begin finalising the database by casting each column to an appropriate data type then relating the tables together so that business analysis could be performed.

#### Casting columns to correct data type

To change the data type of a column, I used the query tool in pgAdmin4 and wrote queries with the following syntax:

```
ALTER TABLE {table_name}
    ALTER COLUMN {column_name} TYPE {data type};
```
This allowed me to correct any instances where a column had been interpreted incorrectly. For example originally the `product_price` column of the `dim_products` table was interpreted as the TEXT data type. As prices are always decimal numbers (e.g. 29.99) I converted the column to the FLOAT data type.

#### Creating the database schema

After this repeating this for each column in the database, I linked the information in the tables together so that business analysis to be performed.

Each table had a column in which each entry was unique and could be used to identify the entry (e.g. the `user_uuid` column of the user data table or the `product_code` of the product data table). This type of column is referred to as a Universally Unique Identifier (UUID). These UUID columns were also present in the orders table, meaning that information could be linked between different tables via the orders table.

For example to find which item was sold for a transaciton made at a specific date and time, I could link the date events table to the orders table via the `date_uuid` column and link the products table to the orders table via the `product_code` UUID. This would allow all me to look at the `date_uuid` of an entry in the date events table, find the row in the orders table with the same `date_uuid`, refer to the corresponding `product_code` and then refer to this `product_code` in the products table to see which product was sold in the transaction.

By linking the tables like this, I created a star-based schema with the orders table at the centre:

![image](https://github.com/LHMak/multinational-retail-data-centralisation75/assets/147920042/62bcb96b-aa63-4675-880b-7a79cb8a6c45)

In the diagram, it can be seen that each table has been linked to the orders table by the UUID columns. To create these relationships I had to set the UUID columns of the store data, card data, product data, date events and user data tables as their respective primary keys. Then I set the UUID columns of the orders table as foreign keys which referenced the primary key columnd of the other tables.

I acomplished this by executing the following queries:

*Setting a column as the primary key*
```
ALTER TABLE [TABLE_NAME]
ADD CONSTRAINT [NAME_FOR_CONSTRAINT]
PRIMARY KEY [COLUMN_NAME];
```

*Setting a column as foreign key in orders table*
```
ALTER TABLE orders_table
ADD CONSTRAINT [NAME_FOR_CONSTRAINT]
FOREIGN KEY [COLUMN_NAME]
REFERENCES [TABLE_NAME] [COLUMN_NAME]);
```
Here is an example of this process in action to link the card data table to the orders table via the `card_number` column:

<img width="995" alt="image" src="https://github.com/LHMak/multinational-retail-data-centralisation75/assets/147920042/df2c8d37-0358-42d5-af95-f6004e6d0f30">

Once I had completed this process for all of the tables, I had successfully centralised all of the company data from each data source and created a robust database which I could now use to perform business analysis.

### Milestone 4: Querying the data.
In this milestone, the goal was to query the new database to answer a set of business questions posed by the senior team of the company. There were 10 questions in total and the `.sql` files for the queries can be found in [Milestone_4_Queries.zip](Milestone_4_Queries.zip)

#### Question 1: How many stores does the business have and in which countries?
Query:
```
SELECT country_code AS "country", count(country_code) AS "total_no_stores"
FROM dim_store_details
WHERE country_code != 'N/A'
GROUP BY country_code;
```

Result:

<img width="239" alt="image" src="https://github.com/LHMak/multinational-retail-data-centralisation75/assets/147920042/3ad6e231-74e9-4d39-b118-72f4daec8201">

The query works by grouping the rows in the store data table by country code (e.g. UK, DE, US) then counting the number of stores in each group. The business also owns an online store, which does not have a country code, so the `WHERE country_code != 'N/A'` clause was included to exclude this from the results.

The resulting table showed the company has 265 stores in Great Britain, 141 in Germany and 34 stores in the United States.

#### Question 2: Which locations currently have the most stores?
Query:
```
SELECT locality, count(locality) AS "total_no_stores"
FROM dim_store_details
WHERE country_code != 'N/A'
GROUP BY locality
ORDER BY count(locality) DESC
LIMIT 7;
```

Result:

<img width="241" alt="image" src="https://github.com/LHMak/multinational-retail-data-centralisation75/assets/147920042/3c9808a2-834d-4e6f-9a78-4feab468ab00">

The query works by grouping all of the entries from the store data table by the `locality` field and counting the number of entries in each group. These counts are then ordered in descending order so that the locality with the most entries appears at the top. Finally, the locality column and the count of stores are returned. As with the last question, the `WHERE country_code != 'N/A'` clause is included to exclude the company's online store.

#### Question 3:

#### Question 4:

#### Question 5:

#### Question 6:

#### Question 7:

#### Question 8:

#### Question 9:

#### Question 10:
## File Structure of Project
- `.gitignore`: Contains list of files which are not tracked by Git. In particular for this project, database credentials are included in the gitignore file and so, are not uploaded to this Github repository.
- `LICENSE`: The License (MIT) file for this project.
- `README.md`: The README markdown file for this project. Contains information about the project's purpose, tools used, file structure, etc.
- `main.py`: This Python script serves are the main controller of the project processes. It works by calling functions from the DatabseConnector, DataExtractor and DataCleaning classes described in the following 3 Python scripts. By using a main.py script, the data that has been extracted by the DataExtractor can be passed to the DataCleaning class; then to the DatabaseConnector to upload to the centralised PostgreSQL database.
- `database_utils.py`: This script introduces the DatabaseConnector class, which is responsible for reading database credentials (in the form of a .YAML file); initialising an SQLAlchemy/psycopg2 engine to manage the connection to a database; listing the tables in a databse to allow selection of data for extraction and finally, uploading cleaned data to the target PostgreSQL database.
- `data_extraction.py`: This script introduces the DataExtractor class, which is responsible for extracting data from a source and generating a pandas dataframe from it if cleaning is required. This class contains 5 extraction functions, which extract from the following source types: RDS tables, PDF documents, APIs, JSON and CSV files.
- `data_cleaning.py`: This script introduces the DataCleaning class, which is responsible for taking in raw data and cleaning it. The data cleaning methods are different for each data source- but typically, null and erroneous entries are identified and removed, typos are corrected and columns are cast to their intended datatypes.
- `Milestone_4_Queries.zip`: This `.zip` folder contains 10 `.sql` files. Each file contains a query to answer one of the questions from a business stakeholder.

## License Information
This project is licensed under the terms of the MIT license.
