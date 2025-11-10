/*
	=================================================================================
	Quality Checks
	=================================================================================
	Script Purpose:
		This script performs vaarious quality checks for data consistency, accuracy
		and standardisation across the 'silver' schema. It includes checks for:
		- Null or duplicate values
		- Unwanted spaces in string fields
		- Data standardisation and consistency
		- Invalid Date Ranges and Orders
		- Data consistency between related fields

	Usage Notes:
		- Run these checks after loading the silver later
		- Investigate and resolver any discrepancies found during the checks

	=================================================================================

*/

--===================================
--Checking the 'silver.crm_cust_info'
--===================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT
	cst_id,
	Count(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT (*) > 1 or CST_ID IS NULL

-- Check for unwanted spaces in all string columns
-- Expectation: No REsults
SELECT cst_firstname FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_gnder FROM bronze.crm_cust_info
WHERE cst_gnder != TRIM(cst_gnder)

SELECT cst_marital_status FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status)

--Check for only most up to date rows being pulled for each productr
--Expectation: No REsults
SELECT * FROM 
(
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
)
t WHERE flag_last = 1

--===================================
--Checking the 'silver.crm_prd_info'
--===================================
-- Checking for duplicates in the primary key
Select
prd_id
From silver.crm_prd_info
GROUP BY prd_id
HAVING Count(*) >1 or prd_id IS NULL

--Checking that the products all have costs. 
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Check foir Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

--===================================
--Checking the 'silver.crm_sales details'
--===================================
--Checking dates fall within range (invalid dates out-of-range)
--Expoectation: No invalid dates
SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 
OR sls_order_dt >20500101 
OR sls_order_dt < 19000101

--Checking that the sales data is not displaying nulls or negative values.
--Expectation: No Nulls Or Negatives
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != (sls_quantity * sls_price)
OR sls_sales IS NULL OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

--Check Data Consistency: Sales = Quantity * Price
--Expectation: No Results
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != (sls_quantity * sls_price)
	OR sls_sales IS NULL
	OR sls_quantity IS NULL 
	OR sls_price IS NULL 
	OR sls_price <= 0
	OR sls_quantity <= 0 
	OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

--===================================
--Checking the 'silver.crm_cust_az12
--===================================
-- Identidy Out-of-Range Dates
-- Expectation: Birthdates between spcified date and today

SELECT DISTINCT
erp_BDATE
FROM silver.erp_cust_az12
WHERE erp_BDATE < '1924-01-01' OR erp_BDATE > GETDATE()


--Data standardisation and consistency
SELECT DISTINCT erp_GEN
FROM silver.erp_cust_az12

--===================================
--Checking the 'silver.crm_loc_a101'
--===================================
--Data Standardisation and consistency
SELECT DISTINCT erp_CNTRY 
FROM silver.erp_loc_a101 
ORDER BY erp_CNTRY

--===================================
--Checking the 'silver.erp_px_cat_gv12'
--===================================
--check for unwanted spaces in other string columns
--Expectations: None
SELECT * 
FROM bronze.erp_px_cat_g1v2
WHERE erp_CAT != TRIM(erp_CAT) 
	OR erp_SUBCAT != TRIM(erp_SUBCAT) 
		OR erp_MAINTENANCE != TRIM(erp_MAINTENANCE)


-- Data standardisation and consistency
SELECT DISTINCT
erp_MAINTENANCE
FROM bronze.erp_px_cat_g1v2
