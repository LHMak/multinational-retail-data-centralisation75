# Multinational Retail Data Centralisation project

## Project Description

I have undertaken this project as part of the AiCore Cloud Engineering pathway. The aim of this project was to collate, clean and centralise the sales data from a fictional multinational company. 

The company sells goods across the globe and their sales data was spread across many different data sources. This meant the data was not easily accessible or analysable.

In order to remedy this issue, this project did the following:

1. Extracted all data from the various sources such as: AWS S3 buckets, AWS cloud databases and APIs hosted on AWS. The data also took many forms such as: PDFs, .json and .csv.
   - This required me to tailor my extraction approach for each datasource. For example, user and order data was stored in a database, while product and payment data was stored in an S3 bucket.
2. Cleaned the raw data, looking for inconsistent formatting (particularly with dates and times), typos and null values.
   - To clean the data I mainly used the Pandas Python library. I also used the re and numpy libraries.
3. Uploaded the cleaned data to a PostgreSQL database, allowing for easy querying and analysis of the data.
   -

## Installation Instructions

## Usage Instructions

## File Structure of Project

