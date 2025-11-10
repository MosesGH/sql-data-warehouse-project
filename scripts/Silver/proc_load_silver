/*
====================================================
Stored Procedure: Load silver layer (Bronze -> Silver)
====================================================
Script Purpose:
	This script performs the ETL (extract, Trnasform, Load) process to populate the
	silver schema tbales from the bronze schema.

	It performs the following actions:
	- Truncates the silver table before loading the data.
	- Uses a range of data cleansing and normalisation procedures on data from the bronze layer.
	- Shows the load time for each table and the total load time.
	- uses a CATCH to check for any errors and print them. 

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values. 

	Usage Example:
		Exec silver.load_silver
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN
DECLARE @silver_whole_batch_start DATETIME, @silver_whole_batch_end DATETIME;
SET @silver_whole_batch_start  = GETDATE();

	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT '=======================';
		PRINT ' Loading Silver Layer';
		PRINT '=======================';

		PRINT'>> ----------------------------'
		PRINT'>> Loading the CRM tables'
		PRINT'>> ----------------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: silver.crm_cust_info'
		Truncate Table silver.crm_cust_info
		PRINT'>> Inserting Data Into: silver.crm_cust_info'

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
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sseconds';
		print'>> ---------------------------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: silver.crm_prd_info'
		Truncate Table silver.crm_prd_info
		PRINT'>> Inserting Data Into: silver.crm_prd_info'

		INSERT INTO silver.crm_prd_info
		(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- cat_id is the name in another table
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		CAST(prd_Start_dt AS DATE) AS prd_start_dt,
		CAST(
			LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 -- lead selects one line ahead
			AS DATE
		) AS prd_end_dt -- calculate end date as one dat before the next start date
		FROM bronze.crm_prd_info
	    SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sseconds';
		print'>> ---------------------------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: silver.crm_sales_details'
		Truncate Table silver.crm_sales_details
		PRINT'>> Inserting Data Into: silver.crm_sales_details'

		INSERT INTO silver.crm_sales_details 
		(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) --cant convert straight to date in sql server
		END as sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) --cant convert straight to date in sql server
		END as sls_ship_dt, -- no errors but will apply anyway, other option is to run checks every day. 
		CASE WHEN sls_due_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) --cant convert straight to date in sql server
		END as sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details


		SET @end_time = GETDATE();
		PRINT'>> ----------------------------'
		PRINT'>> Loading the ERP tables'
		PRINT'>> ----------------------------'
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sseconds';
		print'>> ---------------------------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: silver.erp_cust_az12'
		Truncate Table silver.erp_cust_az12
		PRINT'>> Inserting Data Into: silver.erp_cust_az12'

		INSERT INTO silver.erp_cust_az12
		(erp_CID,erp_BDATE, erp_GEN)
		SELECT
		CASE WHEN erp_CID LIKE 'NAS%' THEN SUBSTRING(erp_CID, 4, LEN(erp_CID))
			ELSE erp_CID
		END erp_CID,
		CASE WHEN erp_BDATE > GETDATE() THEN NULL
			else erp_BDATE
		END AS erp_BDATE,
		CASE WHEN UPPER(TRIM(erp_GEN)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(erp_GEN)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS erp_GEN
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sseconds';
		print'>> ---------------------------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: silver.erp_loc_a101'
		Truncate Table silver.erp_loc_a101
		PRINT'>> Inserting Data Into: silver.erp_loc_a101'

		INSERT INTO silver.erp_loc_a101
		(erp_CID, erp_CNTRY)
		SELECT
		REPLACE(erp_CID, '-', '') erp_CID,
		CASE WHEN TRIM(erp_CNTRY) = 'DE' THEN 'Germany'
			WHEN TRIM(erp_CNTRY) IN ('US' , 'USA') THEN 'United States'
			WHEN TRIM(erp_CNTRY) = '' OR erp_CNTRY IS NULL THEN 'n/a'
			ELSE TRIM(erp_CNTRY)
		END AS erp_CNTRY
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sseconds';
		print'>> ---------------------------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: silver.erp_px_cat_g1v2'
		Truncate Table silver.erp_px_cat_g1v2
		PRINT'>> Inserting Data Into: silver.erp_px_cat_g1v2'

		INSERT INTO silver.erp_px_cat_g1v2
		(erp_ID, erp_CAT, erp_SUBCAT, erp_MAINTENANCE)
		SELECT
		erp_ID,
		erp_CAT,
		erp_SUBCAT,
		erp_MAINTENANCE
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sseconds';
		print'>> ---------------------------------------'
	END TRY
		BEGIN CATCH
			PRINT '========================'
			PRINT 'ERROR OCCURED DURING LOADING OF SILVER LAYER'
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '========================'
		END CATCH
SET @silver_whole_batch_end = GETDATE();
PRINT '>> ---------------------------------------'
PRINT '>> Silver Layer Loading Complete'
PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @silver_whole_batch_start, @silver_whole_batch_end) AS NVARCHAR) + ' seconds';
PRINT '>> ---------------------------------------'
END
