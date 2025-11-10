/*
====================================================
Create Database and Schemas
====================================================
Script Purpose:
	This script loads the entire bronze database using a full load frpm external CSV files.
	IT performs the following actions:
	- Truncates the bronze table before loading the data.
	- Uses the 'BULK INSERT' command to load data from csv files to bronze tables.
	- Shows the load time for each table and the total load time.
	- uses a CATCH to check for any errorrs and print them. 

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values. 

	Usage Example:
		Exec bronze.load_bronze
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @bronze_whole_batch_start DATETIME, @bronze_whole_batch_end DATETIME;
SET @bronze_whole_batch_start  = GETDATE();
PRINT '>> Truncating Table: bronze.crm_cust_info';
PRINT '-------------------------------------------------'
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT '=======================';
		PRINT ' Loading Bronze Layer';
		PRINT '=======================';

		PRINT '-----------------------';
		PRINT ' Loading crm TABLES';
		PRINT '-----------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info -- TRUNCATE empties the table of data
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Moses\Desktop\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK --locking the entire table as it is being loaded
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sseconds';
		print'>> ---------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info -- TRUNCATE empties the table of data
		PRINT '>> Inserting Data Into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Moses\Desktop\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK --locking the entire table as it is being loaded
		);
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details -- TRUNCATE empties the table of data
		PRINT '>> Inserting Data Into: bronze.crm_sales_detailso';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Moses\Desktop\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK --locking the entire table as it is being loaded
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sseconds';
		print'>> ---------------------------------------'

		PRINT '-----------------------';
		PRINT ' Loading ERM TABLES';
		PRINT '-----------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12 -- TRUNCATE empties the table of data
		PRINT '>> Inserting Data Into: erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Moses\Desktop\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK --locking the entire table as it is being loaded
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sseconds';
		print'>> ---------------------------------------'
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a10';
		TRUNCATE TABLE bronze.erp_loc_a101 -- TRUNCATE empties the table of data
		PRINT '>> Inserting Data Into: bronze.erp_loc_a10';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Moses\Desktop\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK --locking the entire table as it is being loaded
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sseconds';
		print'>> ---------------------------------------'
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2 -- TRUNCATE empties the table of data
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Moses\Desktop\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK --locking the entire table as it is being loaded
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print'>> ---------------------------------------'
		END TRY
		BEGIN CATCH
			PRINT '========================'
			PRINT 'ERROR OCCURED DURING LOADING OF BRONZE LAYER'
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '========================'
		END CATCH
SET @bronze_whole_batch_end = GETDATE();
PRINT '>> ---------------------------------------'
PRINT '>> Bronze Layer Loading Complete'
PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @bronze_whole_batch_start,@bronze_whole_batch_end) AS NVARCHAR) + ' seconds';
PRINT '>> ---------------------------------------'
END

--checking the qulaity of the table
--SELECT * FROM bronze.crm_prd_info -- calling whole table to make sure data is loaded correctly
--SELECT COUNT(*) FROM bronze.crm_cust_info -- count to ensure the correct nuumber of rows have come through
