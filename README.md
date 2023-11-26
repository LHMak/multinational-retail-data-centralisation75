# Multinational Retail Data Centralisation project

## Project Description

I have undertaken this project as part of the AiCore Cloud Engineering pathway. The aim of this project was to collate, clean and centralise the sales data from a fictional multinational company. 

The company sells goods across the globe and their sales data was spread across many different data sources. This meant the data was not easily accessible or analysable.

In order to remedy this issue, this project did the following:

1. Extracted all data from the various sources such as: AWS S3 buckets, AWS cloud databases and APIs hosted on AWS. The data also took many forms such as: PDFs, .json and .csv. This required me to tailor my extraction approach for each datasource. For example, user and order data was stored in a database, while product and payment data was stored in an S3 bucket.
1. Cleaned the raw data, looking for inconsistent formatting (particularly with dates and times), typos and null values. To clean the data I mainly used the Pandas Python library. I also used the re and numpy libraries.
1. Uploaded the cleaned data to a PostgreSQL database, allowing for easy querying and analysis of the data. I used PGAdmin as a management tool for the PostgreSQL database.

This project helped me to consolidate everything I have learned throughout the course. I used my knowledge of AWS and APIs to retrieve the data files, my skills with VsCode and Python to write the code to process the data and upload it to the PostgreSQL database and finally, my knowledge skills with Relational Databases and SQL to create the database schema, query and analyse the data. On top of this, I made more use of version control and branching with Git more than ever before- particularly when adding in new features.

Because of this, my confidence with with all of these skills has grown substantially. I have also come to appreciate just how much I have learnt in the last ~1.5 months.

## Tools used

## Installation Instructions

## Usage Instructions

## File Structure of Project

