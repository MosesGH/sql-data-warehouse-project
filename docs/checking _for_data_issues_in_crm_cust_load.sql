--Exec bronze.load_bronze
-- Check foirr Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT * FROM bronze.crm_cust_info

SELECT
cst_id,
Count(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT (*) > 1 or CST_ID IS NULL

-- RESULT, THERE ARE MULTIPLE DUPLICATES AND NULL VALUES FOUND.
/* FIXING THE ISSUES 
The duplicates were as follows
Cust_id No.
29449	2
29473	2
29433	2
NULL	3
29483	2
29466	3
*/

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466 
-- want to get the most up to date one as it has the must up to date information. 


SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
--WHERE cst_id = 29466 
-- Can then check this by using the quesry with a seach for all the non one flags

SELECT * FROM 
(
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
)
t WHERE flag_last != 1 -- t is the alioas of the derived table which can then be referenced in other queries that may want to act on it

-- pulling only the rows where the lines that we have flagged as the most up to date are pulled and using
-- cust_id to check that this has been done for a known duplicate.
SELECT * FROM 
(
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
)
t WHERE flag_last = 1 AND cst_id = 29466


--Check for unwanted spaces in all string columns
-- Expectation: No REsults
SELECT cst_firstname FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_gnder FROM bronze.crm_cust_info
WHERE cst_gnder != TRIM(cst_gnder)

SELECT cst_marital_status FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status)

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
cst_gnder,
cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	) t where flag_last = 1


-- Checking data dtandardisation and consistency
SELECT DISTINCT cst_gnder FROM bronze.crm_cust_info
-- it would be better to use full names rather than abbreviations, so we should change all abbreviations where possible. 

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gnder)) = 'F' THEN 'Female' -- using UPPER to always capitalise encase dat starts to come in in lower case. 
	 WHEN UPPER(TRIM(cst_gnder)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gnder,
cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t where flag_last = 1

-- repeat again for marital status
-- Checking data dtandardisation and consistency
SELECT DISTINCT cst_gnder FROM bronze.crm_cust_info
-- it would be better to use full names rather than abbreviations, so we should change all abbreviations where possible. 

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' -- using UPPER to always capitalise encase dat starts to come in in lower case. 
	 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gnder)) = 'F' THEN 'Female' -- using UPPER to always capitalise encase dat starts to come in in lower case. 
	 WHEN UPPER(TRIM(cst_gnder)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gnder,
cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t where flag_last = 1

-- MAke sure that all dates are actual dates and not NVARCHAR

--adding to the silver tables

INSERT INTO silver.crm_cust_info
(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gnder,
	cst_create_date
)
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' -- using UPPER to always capitalise encase dat starts to come in in lower case. 
	 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gnder)) = 'F' THEN 'Female' -- using UPPER to always capitalise encase dat starts to come in in lower case. 
	 WHEN UPPER(TRIM(cst_gnder)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gnder,
cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t where flag_last = 1


-- now use the same queries to check that the silver is okay
SELECT * FROM silver.crm_cust_info

SELECT
cst_id,
Count(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT (*) > 1 or CST_ID IS NULL

SELECT * FROM 
(
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM silver.crm_cust_info
)
t WHERE flag_last != 1

SELECT cst_firstname FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_gnder FROM silver.crm_cust_info
WHERE cst_gnder != TRIM(cst_gnder)

SELECT cst_marital_status FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status)

SELECT DISTINCT cst_gnder FROM silver.crm_cust_info

Select * FROM silver.crm_cust_info
