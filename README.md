# sql-data-warehouse-project
Buillding a modern data warehouse with SQL sever, including ETL processes, data modelling, and analytics

This project demonstrates a comp[rehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights.

---
# Project Links and Tools
- [Datasets](url): Project Datasets (csv)
- [SQL Sever Management Studio](url)
- [DrawIO](url) - Used to design the data architecture, models, flows and any other technical diagrams throughout the projects
- [Notion](url) - Online project management tool used to outline key goals of the project and tyack progress.
- [Guiding course](url) - A link to the course video

# Project Requirements
## Building the Data Warehouse (Data Engineering)

### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytiucal reporting and informed decision-making. 

### Specifications
**Data Sources**: Import data from two source systenms (ERP and CROM) provided as CSV files.

**Data Quality**: Cleanse and resolve data quality issues proor to analysis..

**Integration**: Combine Both Sourtces into a single, user-friendly data model designed for analytical queries.

**Scope**: Focus on the latest dataset only; hostorisation of data is not required.

**Documentation**: Provide clear documentation of the data model to support both business stakeholders and the analytics teams. 

## BI: Analytics and Reporting (Datya Analytics)
### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behaviour**
- **Product Performance**
- **Sales Trends**

- These insights empower stakeholders with key business metrics, enabling strategic decision-making.

## Data Architecture
The data architecture for the project follows the medallion architecture, Bronze, Silver, and Gold Layers

<img width="1137" height="846" alt="image" src="https://github.com/user-attachments/assets/0b91aa12-115a-4071-a95d-a92eb0b73480" />

- **Bronze**
    - raw and unprocessed data as from sources
    - A point to return to for traceability and debugging.
    - Data is ingested for CSV files in SQL Server Database
- **Silver**
    - Clean and standarised data
    - Basic transformations are applied to prepare the data for analysis
- **Gold**
    - Business Ready Data
    - Provide data to be consumed for reporting and analytics

## Repository Structure
- sql-data-warehouse-project
    - datasets/     #Raw datasets used for the project (ERP and CRM data)
       - souce_crm - Customer Relationship Management Data
       - source_erp - Enterprise Resource Planning Data
    - docs/
       - Catalgue - Data Dictionary For Gold Layer
       - Create Bronze Layer Tables - This script creates the tables on the bronze layer ready for the data to be loaded.
       - Create Database - This script creates a new database namesd 'DataWarehouse'
       - Data Flow.drawio - A Visual Showing how data will flow from the CRM and ERP into and through the different layers of the Arhcitecture.
       - Data Warehouse Architecture.drawio - shows the layers, their object types and the transformations that will be applied to a given layer.
       - Load Bronze Databse.sql - This script loads the entire bronze database using a full load frpm external CSV files
       - Table Keys.drawio - Shows the table keys for ther database, highlighting where the tables of the erp and crm connect internally and to eachother.
       - Checking_for)datta_issues_in... - All of the following ddocuments with similar name type show how the data was pre cleansed of dup;licates, Nulls, Redundant collumns, and potentially eronious data.
       - Creating_gold Views.sql - This script creates views for the Gold layer in the data warehouse,  ProducING a clean, enriched, and business-ready dataset
       - Data_model_star_schema - Shows the connections of the star_schema
       - load_silver_database.sql - This script performs the ETL (extract, Trnasform, Load) process to populate the
	silver schema tbales from the bronze schema.
       - Silver_ddl.sql - This script creates tables in the 'silver' schema, dropping existing
		tables if they already exist - These tables can then be populated
       - Silver_quality_checks.sql - This script performs vaarious quality checks for data consistency, accuracy
		and standardisation across the 'silver' schema
    - Scripts/
       - Bronze
          - Create_bronze_layer_tables.sql
          - load_bronze_database.sql -This script loads the entire bronze database using a full load frpm external CSV files.
          - proc_load_bronze - As above
       - Silver
          - silver_ddl - This script creates tables in the 'silver' schema, dropping existing
		tables if they already exist - These tables can then be populated
          - proc_load_silver - This script performs the ETL (extract, Trnasform, Load) process to populate the silver schema tbales from the bronze schema.
       - Gold
          - ddl_gold - This script creates views for the Gold layer in the data warehouse.
       - init_database - This script creates a new database namesd 'DataWarehouse'
       - init_schemas - Creates schemas for data to be divided into. 
    - Tests/
       - Silver_tests - This script performs vaarious quality checks for data consistency, accuracy
		and standardisation across the 'silver' schema
       - quality_checks_gold - This script performs quality chgecks to calidate the integrity, consistency, and accuracy of the gold layer.
    - README.md - Readme that outlines the project key information
    - LICENCE
## Licence
This project is licensed under the [MIT license](https://opensource.org/license/mit). You are free to use, modify, and share this project with proper attribution.


## About Me
Hi, I'm Moses.

I completed this project to better improve my understanding of how information flows through databases and how to extract data.
