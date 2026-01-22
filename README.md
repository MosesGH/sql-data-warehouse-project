# sql-data-warehouse-project
Buillding a modern data warehouse with SQL sever, including ETL processes, data modelling, and analytics

This project demonstrates a comp[rehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights.

---

# Project Requirements
## Building the Data WArehouse (Data Engineering)

### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytiucal reporting and informed decision-making. 

### Specifications
**Data Sources**: Import data from two source systenms (ERP and CROM) provided as CSV files.

**Data Quality**: Cleanse and resolve data quality issues proor to analysis..

**Integration**: Combine Both Sourtces into a single, user-friendly data model designed for analytical queries.

**Scope**: Focus on the latest dataset only; hostorisation of data is not required.

**Documentation**: Provide clear documentation of the data model to support both business stakeholders and the analytics teams. 

## BI: Analytics and Reporting (Datya Analytics
### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behaviour**
- **Product Performance**
- **Sales Trends**

- These insights empower stakeholders with key business metrics, enabling strategic decision-making.

## Data Architecture
The data architecture for the project follows the medallion architecture, Bronze, Silver, and Gold Layers

<img width="379" height="282" alt="image" src="https://github.com/user-attachments/assets/0b91aa12-115a-4071-a95d-a92eb0b73480" />

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
---

## Licence
This project is licensed under the [MIT license](https://opensource.org/license/mit). You are free to use, modify, and share this project with proper attribution.


## About Me
Hi, I'm Moses.

I completed this project to better inrove my understanding of how information flows through databases and how to extract data and create automated reports.
