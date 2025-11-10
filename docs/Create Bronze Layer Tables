/*
====================================================
Create Database and Schemas
====================================================
Script Purpose:
	This script creates the tables on the bronze layer ready for the data to be loaded:
	- checks for existing tables in the bronnze layer, if they already exist, they are dropped and the tables are recreated. 

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values. 
*/
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL-- 'U' is user defined table
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info
(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gnder NVARCHAR(50),
cst_create_date DATE
)
GO

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL-- 'U' is user defined table
	DROP TABLE bronze.crm_prd_info;
Create Table bronze.crm_prd_info
(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
)
GO

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL-- 'U' is user defined table
	DROP TABLE bronze.crm_sales_details;
Create Table bronze.crm_sales_details
(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
)
GO

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL-- 'U' is user defined table
	DROP TABLE bronze.erp_cust_az12;
Create Table bronze.erp_cust_az12
(
erp_CID NVARCHAR(50),
erp_BDATE DATE,
erp_GEN NVARCHAR(50)
)
GO

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL-- 'U' is user defined table
	DROP TABLE bronze.erp_loc_a101;
Create Table bronze.erp_loc_a101
(
erp_CID NVARCHAR(50),
erp_CNTRY NVARCHAR(50)
)
GO

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL-- 'U' is user defined table
	DROP TABLE bronze.erp_px_cat_g1v2;
Create Table bronze.erp_px_cat_g1v2
(
erp_ID NVARCHAR(50),
erp_CAT NVARCHAR(50),
erp_SUBCAT NVARCHAR(50),
erp_MAINTENANCE NVARCHAR(50)
)
GO

